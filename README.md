# pg-tenant-gate

A small TypeScript service that runs SQL against Postgres for many callers at once, built so that one caller can't make it slow for everyone else. Think of an AI agent firing 40 slow queries at the same moment a person clicks around a dashboard. Both go through the same service and the same pool of connections.

Fastify in front, node-postgres behind, no other dependencies.

## The headline number

Same Postgres 17, same pool of 4 connections. An agent sends 40 queries of 100 ms each at once; 20 ms later a person sends 10 small queries.

| setup | person p50 ms | person p95 ms | person max ms | agent batch done ms |
|:-|-:|-:|-:|-:|
| shared pool, no gate | 1000 | 1001 | 1001 | 1025 |
| with the gate | 4 | 6 | 6 | 2096 |

Without the gate the person waits a full second behind the agent's backlog. With it they get an answer in 6 ms. The price is that the agent's batch takes 2 s instead of 1 s, because it may hold only half the pool. That trade is on purpose: a tenant that floods should feel the limit, not its neighbours. (Numbers from `test/noisy.test.ts`, which prints this table on every run.)

## What the tests prove

Run against Postgres 17.12 and Node 22.23, 20 of 20 pass:

| Test | What could go wrong in production | What the service does |
|---|---|---|
| noisy neighbour | an agent's flood fills the pool, a person's query waits a second | per-tenant cap on connections plus round-robin hand-out: the person waits behind one agent query, not forty |
| round-robin order | free slots go first-come-first-served, so the biggest backlog wins | slots rotate between tenants that are waiting |
| per-tenant share | one tenant takes every connection | at most `perTenantInFlight` at a time, the rest queue |
| token bucket | a caller sends far faster than anyone could need | `429` with `Retry-After`, for that tenant only |
| queue cap | a backlog grows until memory runs out | past `perTenantQueue` the request is shed with `503` |
| caller leaves while queued | a gone caller still takes a slot later | its place is freed and it never reaches Postgres |
| timeout | a slow statement holds a connection forever | `statement_timeout` per request, error `timeout` |
| timeout doesn't leak | a timeout set for one caller stays on the pooled connection for the next | set with `set_config(..., true)`, so it ends with the transaction; the next caller sees `0` |
| caller closes the HTTP connection | the query keeps running on the server after nobody wants it | `pg_cancel_backend` on that backend, checked in `pg_stat_activity` |
| cancel lands late | a cancel sent just as the query ends hits the **next** tenant's query on the same connection | a connection that was sent a cancel is destroyed, never returned to the pool |
| large results | `select` over 5 million rows is built in memory and sent | read through a cursor: 1,000 rows, `truncated: true`, under 200 ms |
| read-only | an agent with read access deletes rows | `BEGIN READ ONLY`; the write fails with `read_only`, rows intact |
| write fails half way | a failed write leaves partial changes | one transaction per request, rolled back on any error |
| multi-statement | `select 1; delete from items` slips a second statement in | refused as `multi_statement`; one statement per request |
| parameters | values pasted into SQL | bound by Postgres, never concatenated |
| unknown fields | `{"sql": ..., "sudo": true}` is accepted and the field silently dropped | refused with `400` (Fastify strips unknown fields by default; this turns that off) |
| metrics | nobody can tell which tenant is hurting the service | request counts and latency per tenant and outcome, plus in-flight and queued gauges, in Prometheus format |

## The contract with callers

`POST /v1/query`, header `x-tenant-id`, body `{ sql, params?, readOnly?, timeoutMs?, maxRows? }`.

- One statement per request, in its own transaction. Success commits; any error rolls back.
- `timeoutMs` defaults to 5 s, capped at 60 s. `maxRows` defaults to 1,000, capped at 10,000. A caller can ask for less, never more.
- Errors always have a `code`: `rate_limited` 429, `overloaded` 503, `timeout` 504, `canceled` 499, `read_only` 403, `multi_statement` 400, `sql_error` 400 with the Postgres `pgCode`.

## How it is built

- [src/gate.ts](src/gate.ts): admission. Token bucket, then queue cap, then the in-flight cap, then round-robin hand-out of free slots.
- [src/run.ts](src/run.ts): one request on one connection. Transaction, local timeout, read-only, cursor, cancel on abort, and destroying a connection that was sent a cancel.
- [src/server.ts](src/server.ts): the HTTP contract and error codes; aborts the query when the client's connection closes.
- [src/metrics.ts](src/metrics.ts): per-tenant counters and latency histogram.

## What a real version still needs

- **Tenant from the token, not a header.** Here `x-tenant-id` is trusted; in production it comes from the verified caller.
- **Fair share by cost, not by count.** A tenant's 30-second query and another's 3 ms query each count as one slot. Weighting by time held would be fairer.
- **Lend idle slots.** When nobody else is waiting, a busy tenant could use more than its share and give it back when someone arrives. That would bring the agent's 2 s back toward 1 s, at the cost of one query's wait for the next caller.
- **Label cardinality.** One metrics series per tenant is fine for a demo; with thousands of tenants, keep series for the busiest few and fold the rest into `other`.
- **Many instances.** The limits here live in one process. Several replicas need either a shared store for the buckets or limits divided per replica.

## Run it

Needs Node 22.18 or newer (it runs the TypeScript directly) and a Postgres:

```
docker run -d --name pg -p 5432:5432 -e POSTGRES_HOST_AUTH_METHOD=trust postgres:17
npm install
PGHOST=127.0.0.1 PGUSER=postgres npm test
```

Without `PGHOST` or `DATABASE_URL` only the 6 gate tests run; the 14 Postgres tests are skipped.
