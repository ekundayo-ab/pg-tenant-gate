// Per-tenant request counts and latency, in Prometheus text format.
// A tenant label is fine for a demo; with thousands of tenants a real service
// would keep per-tenant series only for the busiest few and put the rest in "other".
import type { TenantLoad } from "./gate.ts";

const BUCKETS = [5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000];

type Hist = { counts: number[]; sum: number; n: number };

export class Metrics {
	#requests = new Map<string, number>();
	#latency = new Map<string, Hist>();

	observe(tenant: string, outcome: string, ms: number) {
		const k = `${tenant}\u0000${outcome}`;
		this.#requests.set(k, (this.#requests.get(k) ?? 0) + 1);
		let h = this.#latency.get(tenant);
		if (!h) {
			h = { counts: BUCKETS.map(() => 0), sum: 0, n: 0 };
			this.#latency.set(tenant, h);
		}
		BUCKETS.forEach((b, i) => {
			if (ms <= b) (h as Hist).counts[i]++;
		});
		h.sum += ms;
		h.n++;
	}

	render(load: TenantLoad[]): string {
		const q = (s: string) => JSON.stringify(s);
		const out = ["# TYPE gate_requests_total counter"];
		for (const [k, v] of this.#requests) {
			const [tenant, outcome] = k.split("\u0000");
			out.push(`gate_requests_total{tenant=${q(tenant)},outcome=${q(outcome)}} ${v}`);
		}
		out.push("# TYPE gate_request_ms histogram");
		for (const [tenant, h] of this.#latency) {
			BUCKETS.forEach((b, i) => out.push(`gate_request_ms_bucket{tenant=${q(tenant)},le="${b}"} ${h.counts[i]}`));
			out.push(`gate_request_ms_bucket{tenant=${q(tenant)},le="+Inf"} ${h.n}`);
			out.push(`gate_request_ms_sum{tenant=${q(tenant)}} ${h.sum}`, `gate_request_ms_count{tenant=${q(tenant)}} ${h.n}`);
		}
		out.push("# TYPE gate_in_flight gauge", "# TYPE gate_queued gauge");
		for (const l of load) {
			out.push(`gate_in_flight{tenant=${q(l.tenant)}} ${l.inFlight}`, `gate_queued{tenant=${q(l.tenant)}} ${l.queued}`);
		}
		return `${out.join("\n")}\n`;
	}
}
