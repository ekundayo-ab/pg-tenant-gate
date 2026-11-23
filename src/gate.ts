// Fair admission in front of a shared Postgres pool.
//
// Three limits per tenant, checked in this order:
//   1. a token bucket (ratePerSec, burst): too fast -> rate_limited, with a retry time
//   2. a waiting queue cap (perTenantQueue): too much backlog -> overloaded (load shedding)
//   3. an in-flight cap (perTenantInFlight): one tenant can never hold the whole pool
// Free pool slots go to waiting tenants round-robin, so a tenant with 1 query
// waits behind at most one query of each other tenant, not behind their whole backlog.

export type GateConfig = {
	poolSize: number;
	perTenantInFlight: number;
	perTenantQueue: number;
	ratePerSec: number;
	burst: number;
	now?: () => number;
};

export class GateError extends Error {
	code: "rate_limited" | "overloaded" | "canceled";
	retryAfterMs: number;
	constructor(code: GateError["code"], retryAfterMs = 0) {
		super(code);
		this.code = code;
		this.retryAfterMs = retryAfterMs;
	}
}

type Waiter = {
	resolve: (release: () => void) => void;
	reject: (e: Error) => void;
	signal?: AbortSignal;
	onAbort?: () => void;
};

type Tenant = { tokens: number; last: number; inFlight: number; queue: Waiter[] };

export type TenantLoad = { tenant: string; inFlight: number; queued: number };

export class TenantGate {
	#cfg: Required<GateConfig>;
	#tenants = new Map<string, Tenant>();
	#inFlight = 0;
	// tenants that have at least one waiter, in round-robin order
	#ready: string[] = [];

	constructor(cfg: GateConfig) {
		this.#cfg = { now: () => performance.now(), ...cfg };
	}

	acquire(tenantId: string, signal?: AbortSignal): Promise<() => void> {
		const t = this.#tenant(tenantId);
		if (signal?.aborted) return Promise.reject(new GateError("canceled"));
		this.#refill(t);
		if (t.tokens < 1) {
			const ms = Math.ceil(((1 - t.tokens) / this.#cfg.ratePerSec) * 1000);
			return Promise.reject(new GateError("rate_limited", ms));
		}
		if (t.queue.length >= this.#cfg.perTenantQueue) {
			return Promise.reject(new GateError("overloaded", 1000));
		}
		t.tokens -= 1;
		return new Promise((resolve, reject) => {
			const w: Waiter = { resolve, reject, signal };
			if (signal) {
				w.onAbort = () => {
					const i = t.queue.indexOf(w);
					if (i >= 0) t.queue.splice(i, 1);
					if (t.queue.length === 0) this.#ready = this.#ready.filter((id) => id !== tenantId);
					reject(new GateError("canceled"));
				};
				signal.addEventListener("abort", w.onAbort, { once: true });
			}
			if (t.queue.push(w) === 1) this.#ready.push(tenantId);
			this.#pump();
		});
	}

	load(): TenantLoad[] {
		return [...this.#tenants].map(([tenant, t]) => ({ tenant, inFlight: t.inFlight, queued: t.queue.length }));
	}

	#pump() {
		while (this.#inFlight < this.#cfg.poolSize) {
			const i = this.#ready.findIndex((id) => this.#tenant(id).inFlight < this.#cfg.perTenantInFlight);
			if (i < 0) return;
			const [id] = this.#ready.splice(i, 1);
			const t = this.#tenant(id);
			const w = t.queue.shift() as Waiter;
			if (t.queue.length > 0) this.#ready.push(id);
			if (w.signal && w.onAbort) w.signal.removeEventListener("abort", w.onAbort);
			t.inFlight++;
			this.#inFlight++;
			let released = false;
			w.resolve(() => {
				if (released) return;
				released = true;
				t.inFlight--;
				this.#inFlight--;
				this.#pump();
			});
		}
	}

	#tenant(id: string): Tenant {
		let t = this.#tenants.get(id);
		if (!t) {
			t = { tokens: this.#cfg.burst, last: this.#cfg.now(), inFlight: 0, queue: [] };
			this.#tenants.set(id, t);
		}
		return t;
	}

	#refill(t: Tenant) {
		const now = this.#cfg.now();
		t.tokens = Math.min(this.#cfg.burst, t.tokens + ((now - t.last) / 1000) * this.#cfg.ratePerSec);
		t.last = now;
	}
}
