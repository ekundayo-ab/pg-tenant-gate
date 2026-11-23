// HTTP front: POST /v1/query runs one statement for one tenant; GET /metrics.
// The tenant comes from a header here; in a real service it comes from the
// caller's verified token, never from the request body.
import Fastify from "fastify";
import type pg from "pg";
import { GateError, type TenantGate } from "./gate.ts";
import type { Metrics } from "./metrics.ts";
import { QueryError, runQuery } from "./run.ts";

export type Deps = { pool: pg.Pool; admin: pg.Pool; gate: TenantGate; metrics: Metrics };

const STATUS: Record<string, number> = {
	rate_limited: 429,
	overloaded: 503,
	canceled: 499,
	timeout: 504,
	multi_statement: 400,
	read_only: 403,
	sql_error: 400,
};

const bodySchema = {
	type: "object",
	required: ["sql"],
	additionalProperties: false,
	properties: {
		sql: { type: "string", minLength: 1, maxLength: 100_000 },
		params: { type: "array", maxItems: 1_000 },
		readOnly: { type: "boolean" },
		timeoutMs: { type: "integer", minimum: 1 },
		maxRows: { type: "integer", minimum: 1 },
	},
};

type Body = { sql: string; params?: unknown[]; readOnly?: boolean; timeoutMs?: number; maxRows?: number };

export function buildServer(d: Deps) {
	// Fastify's default validator silently strips unknown fields (removeAdditional).
	// For a service that runs SQL for its callers, an unknown field is a caller bug: refuse it.
	const app = Fastify({ ajv: { customOptions: { removeAdditional: false } } });

	app.post<{ Body: Body }>("/v1/query", { schema: { body: bodySchema } }, async (request, reply) => {
		const tenant = request.headers["x-tenant-id"];
		if (typeof tenant !== "string" || tenant === "") {
			return reply.code(400).send({ error: { code: "no_tenant" } });
		}
		const t0 = performance.now();
		const ac = new AbortController();
		reply.raw.on("close", () => {
			if (!reply.raw.writableFinished) ac.abort();
		});
		const fail = (code: string, message: string, extra: Record<string, unknown> = {}) => {
			d.metrics.observe(tenant, code, performance.now() - t0);
			return reply.code(STATUS[code] ?? 500).send({ error: { code, message, ...extra } });
		};

		let release: () => void;
		try {
			release = await d.gate.acquire(tenant, ac.signal);
		} catch (e) {
			if (!(e instanceof GateError)) throw e;
			if (e.retryAfterMs) reply.header("retry-after", Math.ceil(e.retryAfterMs / 1000));
			return fail(e.code, e.message, e.retryAfterMs ? { retryAfterMs: e.retryAfterMs } : {});
		}
		try {
			const result = await runQuery(d.pool, d.admin, { ...request.body, signal: ac.signal });
			d.metrics.observe(tenant, "ok", performance.now() - t0);
			return result;
		} catch (e) {
			if (!(e instanceof QueryError)) throw e;
			return fail(e.code, e.message, e.pgCode ? { pgCode: e.pgCode } : {});
		} finally {
			release();
		}
	});

	app.get("/metrics", async (_req, reply) => {
		reply.type("text/plain; version=0.0.4");
		return d.metrics.render(d.gate.load());
	});

	return app;
}
