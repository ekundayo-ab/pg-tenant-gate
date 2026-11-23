// The headline test: an agent floods the service while a person runs small queries.
// Same Postgres, same pool of 4 connections, with and without the gate.
import assert from "node:assert/strict";
import { test } from "node:test";
import pg from "pg";
import { TenantGate } from "../src/gate.ts";
import { runQuery } from "../src/run.ts";

// Connection from DATABASE_URL or the standard PGHOST/PGUSER/PGDATABASE variables.
const url = process.env.DATABASE_URL;
const hasDb = Boolean(url || process.env.PGHOST);
const opts = { skip: hasDb ? false : "set DATABASE_URL or PGHOST to run against Postgres" };

const AGENT_QUERIES = 40; // each sleeps 100 ms: 1 s of work for a pool of 4
const HUMAN_QUERIES = 10;

const pct = (xs: number[], p: number) => {
	const s = [...xs].sort((a, b) => a - b);
	return Math.round(s[Math.min(s.length - 1, Math.ceil((p / 100) * s.length) - 1)]);
};

async function scenario(run: (tenant: string, sql: string) => Promise<unknown>) {
	const t0 = performance.now();
	const agent = Array.from({ length: AGENT_QUERIES }, () => run("agent", "select pg_sleep(0.1)"));
	await new Promise((r) => setTimeout(r, 20));
	const human = await Promise.all(
		Array.from({ length: HUMAN_QUERIES }, async () => {
			const s = performance.now();
			await run("human", "select 1");
			return performance.now() - s;
		}),
	);
	await Promise.all(agent);
	return { humanP50: pct(human, 50), humanP95: pct(human, 95), humanMax: pct(human, 100), agentTotal: Math.round(performance.now() - t0) };
}

test("noisy neighbour: the gate keeps a person's queries fast while an agent floods", opts, async () => {
	const naivePool = new pg.Pool({ connectionString: url, max: 4 });
	const naive = await scenario((_t, sql) => naivePool.query(sql));
	await naivePool.end();

	const pool = new pg.Pool({ connectionString: url, max: 4 });
	const admin = new pg.Pool({ connectionString: url, max: 1 });
	const gate = new TenantGate({ poolSize: 4, perTenantInFlight: 2, perTenantQueue: 100, ratePerSec: 1000, burst: 1000 });
	const gated = await scenario(async (tenant, sql) => {
		const release = await gate.acquire(tenant);
		try {
			return await runQuery(pool, admin, { sql });
		} finally {
			release();
		}
	});
	await pool.end();
	await admin.end();

	console.log("\n| setup | person p50 ms | person p95 ms | person max ms | agent batch done ms |");
	console.log("|:-|-:|-:|-:|-:|");
	console.log(`| shared pool, no gate | ${naive.humanP50} | ${naive.humanP95} | ${naive.humanMax} | ${naive.agentTotal} |`);
	console.log(`| with the gate | ${gated.humanP50} | ${gated.humanP95} | ${gated.humanMax} | ${gated.agentTotal} |`);

	assert.ok(naive.humanP95 > 700, `without the gate the person should wait behind the agent (p95 ${naive.humanP95})`);
	assert.ok(gated.humanP95 < 150, `with the gate the person stays fast (p95 ${gated.humanP95})`);
});
