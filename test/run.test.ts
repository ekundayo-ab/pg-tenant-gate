import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import pg from "pg";
import { type QueryError, runQuery } from "../src/run.ts";

// Connection from DATABASE_URL or the standard PGHOST/PGUSER/PGDATABASE variables.
const url = process.env.DATABASE_URL;
const hasDb = Boolean(url || process.env.PGHOST);
const opts = { skip: hasDb ? false : "set DATABASE_URL or PGHOST to run against Postgres" };

let pool: pg.Pool;
let admin: pg.Pool;
const count = async () => Number((await admin.query("select count(*) from items")).rows[0].count);
const code = (c: string) => (e: QueryError) => e.code === c;

before(async () => {
	if (!hasDb) return;
	pool = new pg.Pool({ connectionString: url, max: 1 });
	admin = new pg.Pool({ connectionString: url, max: 2 });
	await admin.query("drop table if exists items; create table items (id int primary key); insert into items values (1), (2), (3)");
});
after(async () => {
	await pool?.end();
	await admin?.end();
});

test("timeout: a slow statement stops at its timeout", opts, async () => {
	const t0 = performance.now();
	await assert.rejects(runQuery(pool, admin, { sql: "select pg_sleep(5)", timeoutMs: 300 }), code("timeout"));
	assert.ok(performance.now() - t0 < 1500);
});

test("the timeout does not leak to the next caller on the same connection", opts, async () => {
	await assert.rejects(runQuery(pool, admin, { sql: "select pg_sleep(5)", timeoutMs: 200 }), code("timeout"));
	const r = await pool.query("show statement_timeout");
	assert.equal(r.rows[0].statement_timeout, "0");
});

test("large results: 5 million rows asked, 1,000 read, truncated flagged", opts, async () => {
	const r = await runQuery(pool, admin, { sql: "select g from generate_series(1, 5000000) g", maxRows: 1000 });
	assert.equal(r.rowCount, 1000);
	assert.equal(r.truncated, true);
	assert.ok(r.ms < 1000, `took ${r.ms} ms`);
});

test("read-only requests cannot write", opts, async () => {
	await assert.rejects(runQuery(pool, admin, { sql: "delete from items", readOnly: true }), code("read_only"));
	assert.equal(await count(), 3);
});

test("a write commits; a failing write rolls back", opts, async () => {
	const r = await runQuery(pool, admin, { sql: "insert into items values ($1) returning id", params: [4] });
	assert.deepEqual(r.rows, [{ id: 4 }]);
	assert.equal(await count(), 4);
	await assert.rejects(runQuery(pool, admin, { sql: "insert into items select 10 / 0" }), (e: QueryError) => e.code === "sql_error" && e.pgCode === "22012");
	assert.equal(await count(), 4);
});

test("one statement per request", opts, async () => {
	await assert.rejects(runQuery(pool, admin, { sql: "select 1; delete from items" }), code("multi_statement"));
	assert.equal(await count(), 4);
});

test("parameters are bound, not pasted into the SQL", opts, async () => {
	const r = await runQuery(pool, admin, { sql: "select $1::text as v", params: ["'); drop table items; --"] });
	assert.equal(r.rows[0].v, "'); drop table items; --");
	assert.equal(await count(), 4);
});

test("caller leaves: the statement is cancelled on the server and the connection is not reused", opts, async () => {
	const own = new pg.Pool({ connectionString: url, max: 1 });
	const ac = new AbortController();
	setTimeout(() => ac.abort(), 200);
	const t0 = performance.now();
	await assert.rejects(runQuery(own, admin, { sql: "select pg_sleep(7)", timeoutMs: 30_000, signal: ac.signal }), code("canceled"));
	assert.ok(performance.now() - t0 < 1500);
	const left = await admin.query("select count(*) from pg_stat_activity where state = 'active' and query = 'select pg_sleep(7)'");
	assert.equal(Number(left.rows[0].count), 0);
	assert.equal(own.totalCount, 0, "the connection that got a cancel was destroyed");
	await own.end();
});
