import pg from "pg";
import { TenantGate } from "./gate.ts";
import { Metrics } from "./metrics.ts";
import { buildServer } from "./server.ts";

const poolSize = Number(process.env.POOL_SIZE ?? 8);

const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL, max: poolSize });
const admin = new pg.Pool({ connectionString: process.env.DATABASE_URL, max: 1 });
const gate = new TenantGate({ poolSize, perTenantInFlight: Math.max(1, poolSize / 4), perTenantQueue: 50, ratePerSec: 20, burst: 40 });
const app = buildServer({ pool, admin, gate, metrics: new Metrics() });

console.log(`listening on ${await app.listen({ port: Number(process.env.PORT ?? 3000), host: "127.0.0.1" })}`);
