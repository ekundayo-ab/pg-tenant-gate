// Runs one caller's SQL on a pooled connection, with the contract written down:
//
// - one statement per request; "select 1; select 2" is refused (multi_statement)
// - a transaction per request: committed on success, rolled back on any error
// - statement_timeout is set with SET LOCAL semantics, so it ends with the
//   transaction and never leaks to the next caller of the same connection
// - read-only requests run in BEGIN READ ONLY; a write fails with read_only
// - results are read through a cursor, at most maxRows + 1 rows; the server
//   never builds the full result in memory, and the caller gets truncated: true
// - if the caller goes away, the running statement is cancelled on the server
//   (pg_cancel_backend), and that connection is destroyed instead of reused,
//   because a cancel that lands late would otherwise hit the next tenant's query
import type pg from "pg";
import Cursor from "pg-cursor";

export type QueryRequest = {
	sql: string;
	params?: unknown[];
	readOnly?: boolean;
	timeoutMs?: number;
	maxRows?: number;
	signal?: AbortSignal;
};

export type QueryResult = { rows: Record<string, unknown>[]; rowCount: number; truncated: boolean; ms: number };

export type QueryErrorCode = "timeout" | "canceled" | "multi_statement" | "read_only" | "sql_error";

export class QueryError extends Error {
	code: QueryErrorCode;
	pgCode?: string;
	constructor(code: QueryErrorCode, message: string, pgCode?: string) {
		super(message);
		this.code = code;
		this.pgCode = pgCode;
	}
}

export const LIMITS = { timeoutMs: { def: 5_000, max: 60_000 }, maxRows: { def: 1_000, max: 10_000 } };

const clamp = (v: number | undefined, d: { def: number; max: number }) =>
	Math.min(Math.max(1, Math.floor(v ?? d.def)), d.max);

export async function runQuery(pool: pg.Pool, admin: pg.Pool, req: QueryRequest): Promise<QueryResult> {
	const t0 = performance.now();
	const timeoutMs = clamp(req.timeoutMs, LIMITS.timeoutMs);
	const maxRows = clamp(req.maxRows, LIMITS.maxRows);
	if (req.signal?.aborted) throw new QueryError("canceled", "caller went away before the query started");

	const client = await pool.connect();
	// biome-ignore lint: processID is set by pg after connect but missing from its types
	const pid = (client as any).processID as number;
	let cancelSent = false;
	let running = false;
	const onAbort = () => {
		if (!running || cancelSent) return;
		cancelSent = true;
		admin.query("select pg_cancel_backend($1)", [pid]).catch(() => {});
	};
	let destroy: Error | undefined;
	try {
		await client.query(req.readOnly ? "begin read only" : "begin");
		await client.query("select set_config('statement_timeout', $1, true)", [`${timeoutMs}ms`]);
		req.signal?.addEventListener("abort", onAbort, { once: true });
		running = true;
		if (req.signal?.aborted) onAbort();
		const cursor = client.query(new Cursor(req.sql, req.params ?? []));
		let rows: Record<string, unknown>[];
		try {
			rows = await cursor.read(maxRows + 1);
		} finally {
			await cursor.close().catch(() => {});
		}
		running = false;
		await client.query("commit");
		const truncated = rows.length > maxRows;
		if (truncated) rows = rows.slice(0, maxRows);
		return { rows, rowCount: rows.length, truncated, ms: Math.round(performance.now() - t0) };
	} catch (e) {
		running = false;
		await client.query("rollback").catch((err: Error) => {
			destroy = err;
		});
		throw toQueryError(e, cancelSent);
	} finally {
		req.signal?.removeEventListener("abort", onAbort);
		if (cancelSent) destroy ??= new Error("cancel was sent on this connection");
		client.release(destroy);
	}
}

function toQueryError(e: unknown, cancelSent: boolean): QueryError {
	if (e instanceof QueryError) return e;
	const err = e as { code?: string; message?: string };
	const msg = err.message ?? String(e);
	switch (err.code) {
		case "57014":
			return cancelSent
				? new QueryError("canceled", "caller went away; statement cancelled on the server", err.code)
				: new QueryError("timeout", "statement timeout", err.code);
		case "25006":
			return new QueryError("read_only", msg, err.code);
		case "42601":
			if (/multiple commands/.test(msg)) return new QueryError("multi_statement", "send one statement per request", err.code);
	}
	return new QueryError("sql_error", msg, err.code);
}
