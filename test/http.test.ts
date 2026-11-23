import assert from "node:assert/strict";
import type { AddressInfo } from "node:net";
import { after, before, test } from "node:test";
import pg from "pg";
import { TenantGate } from "../src/gate.ts";
import { Metrics } from "../src/metrics.ts";
import { buildServer } from "../src/server.ts";

// Connection from DATABASE_URL or the standard PGHOST/PGUSER/PGDATABASE variables.
const url = process.env.DATABASE_URL;
const hasDb = Boolean(url || process.env.PGHOST);
const opts = { skip: hasDb ? false : "set DATABASE_URL or PGHOST to run against Postgres" };

let base = "";
let app: ReturnType<typeof buildServer>;
let pool: pg.Pool;
let admin: pg.Pool;

before(async () => {
	if (!hasDb) return;
	pool = new pg.Pool({ connectionString: url, max: 4 });
	admin = new pg.Pool({ connectionString: url, max: 2 });
	const gate = new TenantGate({ poolSize: 4, perTenantInFlight: 2, perTenantQueue: 10, ratePerSec: 0.01, burst: 3 });
	app = buildServer({ pool, admin, gate, metrics: new Metrics() });
	await app.listen({ port: 0, host: "127.0.0.1" });
	base = `http://127.0.0.1:${(app.server.address() as AddressInfo).port}`;
});
after(async () => {
	await app?.close();
	await pool?.end();
	await admin?.end();
});

const query = (tenant: string | null, body: unknown, signal?: AbortSignal) =>
	fetch(`${base}/v1/query`, {
		method: "POST",
		headers: { "content-type": "application/json", ...(tenant ? { "x-tenant-id": tenant } : {}) },
		body: JSON.stringify(body),
		signal,
	});

test("a query returns rows", opts, async () => {
	const r = await query("t1", { sql: "select 41 + 1 as n" });
	assert.equal(r.status, 200);
	assert.deepEqual((await r.json()).rows, [{ n: 42 }]);
});

test("bad requests are refused before they reach Postgres", opts, async () => {
	assert.equal((await query(null, { sql: "select 1" })).status, 400);
	assert.equal((await query("t2", { sql: "select 1", sudo: true })).status, 400);
});

test("rate limit: 429 with Retry-After for that tenant only", opts, async () => {
	for (let i = 0; i < 3; i++) assert.equal((await query("burst", { sql: "select 1" })).status, 200);
	const r = await query("burst", { sql: "select 1" });
	assert.equal(r.status, 429);
	assert.ok(Number(r.headers.get("retry-after")) > 0);
	assert.equal((await r.json()).error.code, "rate_limited");
	assert.equal((await query("calm", { sql: "select 1" })).status, 200);
});

test("closed connection: the statement is cancelled on the server", opts, async () => {
	const ac = new AbortController();
	setTimeout(() => ac.abort(), 200);
	await assert.rejects(query("leaver", { sql: "select pg_sleep(9)", timeoutMs: 30_000 }, ac.signal));
	let active = -1;
	for (let i = 0; i < 20 && active !== 0; i++) {
		await new Promise((r) => setTimeout(r, 100));
		const q = await admin.query("select count(*) from pg_stat_activity where state = 'active' and query = 'select pg_sleep(9)'");
		active = Number(q.rows[0].count);
	}
	assert.equal(active, 0);
});

test("metrics: per tenant and per outcome", opts, async () => {
	const text = await (await fetch(`${base}/metrics`)).text();
	assert.match(text, /gate_requests_total\{tenant="t1",outcome="ok"\} 1/);
	assert.match(text, /gate_requests_total\{tenant="burst",outcome="rate_limited"\} 1/);
	assert.match(text, /gate_requests_total\{tenant="leaver",outcome="canceled"\} 1/);
	assert.match(text, /gate_request_ms_count\{tenant="calm"\} 1/);
});
