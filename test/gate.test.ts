import assert from "node:assert/strict";
import { test } from "node:test";
import { GateError, TenantGate } from "../src/gate.ts";

const tick = () => new Promise((r) => setImmediate(r));

test("round-robin: a tenant with one query waits behind one query of a busy tenant, not its whole backlog", async () => {
	const gate = new TenantGate({ poolSize: 1, perTenantInFlight: 1, perTenantQueue: 100, ratePerSec: 1000, burst: 1000 });
	const order: string[] = [];
	const releases = new Map<string, () => void>();
	const ask = (tenant: string, name: string) =>
		gate.acquire(tenant).then((rel) => {
			order.push(name);
			releases.set(name, rel);
		});
	for (let i = 1; i <= 6; i++) ask("agent", `a${i}`);
	ask("human", "b1");
	await tick();
	for (const next of ["a1", "a2", "b1", "a3"]) {
		releases.get(next)?.();
		await tick();
	}
	assert.deepEqual(order.slice(0, 4), ["a1", "a2", "b1", "a3"]);
});

test("one tenant never holds more than its share of the pool", async () => {
	const gate = new TenantGate({ poolSize: 4, perTenantInFlight: 2, perTenantQueue: 100, ratePerSec: 1000, burst: 1000 });
	let agent = 0;
	for (let i = 0; i < 4; i++) gate.acquire("agent").then(() => agent++);
	let human = 0;
	gate.acquire("human").then(() => human++);
	await tick();
	assert.equal(agent, 2);
	assert.equal(human, 1);
	assert.deepEqual(
		gate.load().find((l) => l.tenant === "agent"),
		{ tenant: "agent", inFlight: 2, queued: 2 },
	);
});

test("token bucket: too fast gets rate_limited with a retry time, then recovers", async () => {
	let now = 0;
	const gate = new TenantGate({ poolSize: 10, perTenantInFlight: 10, perTenantQueue: 100, ratePerSec: 1, burst: 3, now: () => now });
	for (let i = 0; i < 3; i++) await gate.acquire("t");
	await assert.rejects(gate.acquire("t"), (e: GateError) => e.code === "rate_limited" && e.retryAfterMs === 1000);
	await assert.doesNotReject(gate.acquire("other"));
	now = 1000;
	await assert.doesNotReject(gate.acquire("t"));
});

test("queue cap: a backlog past the cap is shed as overloaded", async () => {
	const gate = new TenantGate({ poolSize: 1, perTenantInFlight: 1, perTenantQueue: 2, ratePerSec: 1000, burst: 1000 });
	await gate.acquire("t");
	gate.acquire("t");
	gate.acquire("t");
	await assert.rejects(gate.acquire("t"), (e: GateError) => e.code === "overloaded");
});

test("a caller that leaves while waiting frees its place and takes no slot", async () => {
	const gate = new TenantGate({ poolSize: 1, perTenantInFlight: 1, perTenantQueue: 10, ratePerSec: 1000, burst: 1000 });
	const release = await gate.acquire("a");
	const ac = new AbortController();
	const waiting = gate.acquire("b", ac.signal);
	ac.abort();
	await assert.rejects(waiting, (e: GateError) => e.code === "canceled");
	assert.equal(gate.load().find((l) => l.tenant === "b")?.queued, 0);
	let got = false;
	gate.acquire("c").then(() => {
		got = true;
	});
	release();
	await tick();
	assert.equal(got, true);
});

test("releasing twice does not free a second slot", async () => {
	const gate = new TenantGate({ poolSize: 1, perTenantInFlight: 1, perTenantQueue: 10, ratePerSec: 1000, burst: 1000 });
	const release = await gate.acquire("a");
	let granted = 0;
	gate.acquire("b").then(() => granted++);
	gate.acquire("c").then(() => granted++);
	release();
	release();
	await tick();
	assert.equal(granted, 1);
});
