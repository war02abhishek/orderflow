# OrderFlow: A Flash-Sale Order Platform — Hands-On Scale/Mesh/Canary Lab

> System design: [OrderFlow Blueprint](https://claude.ai/code/artifact/9e546f02-a666-468e-b38a-c145fb78273f) (architecture diagram, checkout saga sequence, CAP trade-off, failure/circuit-breaker handling, and the gap analysis referenced below as G1–G18)
>
> Learning walkthrough: [OrderFlow Field Notes](https://claude.ai/code/artifact/d7356850-5a1b-44c4-a92d-e917a24d0118) — start-to-finish, concept-first explanation of everything built through Phase 2 (nodes/clusters, NetworkPolicy/Calico, every K8s Service type, the Kafka/Outbox saga, real failure-handling proof, a guided `kubectl` tour), backed by actual verified output from the running cluster rather than hypothetical examples

**Status:** Phases 0–2 complete and verified against a live cluster. Phase 3 (plain Ingress baseline) is next.

- Phase 0: kind cluster `orderflow` (1 control-plane + 2 workers) with Calico as the CNI (kindnetd doesn't enforce NetworkPolicy — swapped it out so Phase 1's policies are real), a local image registry, Homebrew/kind/helm/istioctl installed without sudo.
- Phase 1: `orders` + `inventory` (Spring Boot, Postgres), atomic stock reservation (G1) and idempotent reserve/release (G2) both verified live under concurrent load, readiness/liveness probes + resource limits + pod anti-affinity + graceful shutdown (G8–G10, G12) all in place, default-deny NetworkPolicy per namespace, every K8s Service type demonstrated (ClusterIP, NodePort, Headless, LoadBalancer via MetalLB, ExternalName), checkout console v1 deployed.
- Phase 2: 3-broker Kafka via Strimzi (KRaft, no ZooKeeper) survives a broker being killed mid-saga (G7, verified live); `payment` and `notifications` built, both consuming with `CooperativeStickyAssignor` and a Redis-fast-path + Postgres-backstop idempotency check (G17, caught real redeliveries live during rollout restarts); the Outbox pattern in `orders` (outbox table + scheduled relay, `acks=all` + idempotent producer per G3) drives the full saga — checkout → `AWAITING_PAYMENT` → payment succeeds (`CONFIRMED`) or fails (compensating `inventory` release → `CANCELLED`) → `notifications` logs the outcome — verified end-to-end for both paths. Consistency model written up in `docs/07-saga-and-outbox.md` (G16).
- **Known environment caveat (Phase 1, still applies):** MetalLB assigns a real external IP correctly (the K8s-level lesson), but L2 packet delivery to that IP doesn't work on Docker Desktop for Mac's virtualized networking (confirmed via ARP: the ip neigh table never resolves the VIP) — a documented Docker-Desktop-for-Mac limitation, not a MetalLB or cluster misconfiguration. Works as expected on a native Linux Docker host.
- **Lessons learned during Phase 2** (worth knowing if you hit them again): Spring Boot 4's `spring-boot-starter-webmvc` doesn't transitively provide an injectable `ObjectMapper` bean the way older `spring-boot-starter-web` did — needed an explicit `@Bean ObjectMapper` in every service that does manual JSON (de)serialization for Kafka messages. `imagePullPolicy` defaults to `IfNotPresent`, so redeploying a rebuilt image under the same `:dev` tag silently reused the stale cached image until `imagePullPolicy: Always` was added to every Deployment. Hibernate's `ddl-auto: update` doesn't retroactively fix an existing CHECK constraint (or a `@Lob` column's underlying type) when a Java enum or annotation changes — needed a manual `ALTER TABLE`/`DROP TABLE` once per schema change. Default HikariCP pool size (10) × 4 services × several replicas exceeded Postgres's default `max_connections=100` — fixed by capping each service's pool at 5 and raising Postgres to 300.

## Context

You've already built a microservices project (ticket app: multiple services,
a service registry, an API gateway, Kafka, Docker, basic Kubernetes) and want
to go deeper into what makes systems *actually* scale in production, not just
run as a prototype: service mesh vs. service registry, canary releases, the
different Kubernetes Service types and when to use each, namespaces as
network boundaries, how pods discover and talk to each other, how replicas
get load balanced, and CI/CD driving all of it.

This plan went through three rounds of feedback:
1. The first draft was too abstract — generic CRUD services with a checklist
   of K8s features bolted on. The ask was a concrete problem with visible
   learning at each step, and confirmation that Kafka and Grafana should both
   be real, not decorative.
2. Next: **Spring Boot** instead of Go (stronger familiarity), and an
   evaluation of whether **Saga** and **Outbox** patterns fit — not forced
   in, only if genuinely useful for "critical info."
3. Before writing any code: the system design up front, and a check for what
   a genuinely scalable, consistent, fault-tolerant, available system needs
   beyond what was already listed. That check (written up as the
   [OrderFlow Blueprint](https://claude.ai/code/artifact/9e546f02-a666-468e-b38a-c145fb78273f)
   artifact) found 16 concrete gaps — from an overselling race condition to a
   single-broker Kafka SPOF — folded into the phases below as **G1–G16**.
4. A follow-up asked specifically whether Kafka consumer-group rebalancing
   and Redis needed to be part of the design, and asked for the CAP
   trade-off and failure-handling behavior to be made visual rather than
   left as prose. That pass (same Blueprint artifact, now v3) found two more
   gaps — **G17** (Kafka redelivery after a rebalance isn't handled
   idempotently) and **G18** (a per-replica rate limit quietly becomes
   "limit × replica count" once the ingress gateway is HA) — and confirmed
   Redis has exactly two jobs here (an idempotency fast-path, and optionally
   shared rate-limit counters), not a general-purpose cache.
5. Last: whether a frontend would help make the system easier to visualize
   and understand. A full frontend was the wrong shape — it doesn't teach
   anything on the original list, and it can't show mesh traffic, canary
   splits, or circuit breaking the way Grafana/Kiali/Jaeger already do. What
   those tools *don't* show is the journey of one specific order. So
   instead: a tiny static **checkout console** (plain HTML/JS, no framework,
   no build step) that calls `POST /checkout` and polls `GET /orders/{id}`,
   so the saga's state transitions, the responding pod, and (later) the
   canary version are things to watch happen instead of infer from logs.

Saga and Outbox fit well here, and they make the Kafka usage *stronger*, not
just present: checkout naturally becomes a multi-step distributed transaction
(reserve stock → charge payment → confirm order) with a real failure mode to
compensate for, and the events driving that need reliable delivery. That's
exactly what Saga + Outbox are for. Details below.

**Confirmed decisions:**
- Cluster target: **local** (kind), not a paid cloud cluster
- Service mesh: **Istio**
- Services: **fresh, minimal**, built with **Spring Boot** (Java)
- CI/CD: **Jenkins**, self-hosted
- **Kafka**: yes — carries the saga's commands/events, not just a
  side notification
- **Grafana**: yes — custom dashboards, backed by Spring Boot
  Actuator/Micrometer + Istio's Envoy metrics in Prometheus
- **Saga (orchestrated) + Outbox**: yes, applied specifically to the
  payment step of checkout (see below) — not applied everywhere it isn't
  needed

## The problem statement

**OrderFlow is a checkout platform that must survive a flash sale without
falling over, correctly complete or roll back a multi-step transaction
(stock → payment → confirmation) when something fails mid-flight, and
safely ship a risky new checkout algorithm while all of that is happening.**
Every phase either builds toward this scenario or exercises it directly.

**The checkout saga** (this is the "critical info" Kafka carries):
1. `orders` receives a checkout request, calls `inventory` **synchronously**
   to reserve stock (still fast/blocking — a real checkout needs to know
   immediately if the item is available; no saga needed for this step, it's
   a simple precondition).
2. On success, `orders` creates an `Order` row (`status=AWAITING_PAYMENT`)
   **and** an outbox row for a `PaymentRequested` command **in the same DB
   transaction** — this is the **Outbox pattern**: the DB write and the
   intent to publish to Kafka become atomic, so a crash between "order
   created" and "event published" can never lose or fabricate an event. A
   scheduled outbox relay polls the table and publishes unpublished rows to
   Kafka, then marks them sent.
3. `payment` (new service) consumes `PaymentRequested`, simulates processing
   (configurable failure rate so both paths can be forced), and publishes
   `PaymentCompleted` or `PaymentFailed`.
4. `orders`, acting as the **saga orchestrator**, consumes the result:
   - **Success** → order `CONFIRMED`, outbox-publish `OrderPlaced` (same
     outbox mechanism) for `notifications` to pick up.
   - **Failure** → **compensating transaction**: `orders` calls
     `inventory`'s `/release` endpoint synchronously to free the reserved
     stock, order `CANCELLED`, outbox-publish `OrderCancelled` so the
     customer still gets told.

This is a textbook orchestrated saga with a real compensating action, and it
gives Kafka an actual job — carrying the events a crash or redelivery must
not corrupt — rather than being a decorative notification bus.

**Then, layered on top of that saga:**
- Flash-sale traffic spike (10x) must be absorbed via **autoscaling**, watched
  live on a Grafana dashboard.
- Under that same load, `inventory` (called synchronously, step 1 above)
  must not be allowed to cascade into checkout failures if it degrades —
  **Istio circuit breaking/retries/timeouts** contain the blast radius.
- Mid-sale, `orders` v2 (a new discount-calculation path) is shipped as a
  **canary**: 10% of live traffic, error rate/latency watched **per version**
  on a second Grafana dashboard, promoted or rolled back based on that data.

Each phase below states its **learning outcome**: what should be demonstrable
and explainable afterward, not just a feature that was installed.

## Why this stack

- **Spring Boot** for all four services — stronger existing familiarity, and
  it gives two direct pedagogical wins: Spring Boot Actuator + Micrometer
  expose Prometheus metrics with almost no code, which is what feeds the
  Grafana dashboards; and if the earlier ticket app used Spring Cloud
  (Eureka + Spring Cloud Gateway, common for that stack),
  `docs/03-service-registry-vs-mesh.md` becomes a same-language,
  apples-to-apples comparison instead of a cross-language guess.
- **Postgres**, one instance in a `data` namespace with a separate
  schema/database per service (`orders`, `inventory`, `payment`) — a real
  team would usually run one DB per service; sharing one instance here is a
  deliberate scope trade-off for a learning cluster, called out explicitly
  in `docs/02-namespaces-networking.md` rather than hidden. `orders` and
  `payment` need it for outbox tables and idempotency tracking; `inventory`
  needs it for stock counts.
- **Kafka**, a **3-broker** cluster (via Strimzi) in its own `kafka`
  namespace, replication factor 3 / `min.insync.replicas=2`, carrying
  `PaymentRequested` / `PaymentCompleted` / `PaymentFailed` / `OrderPlaced` /
  `OrderCancelled`. A single broker was the original plan but is a flat SPOF
  (G7) — 3 small brokers are feasible on local kind and directly earn the
  "fault-tolerant" claim rather than just gesturing at it.
- **Redis**, in its own `cache` namespace, used for exactly two things and
  nothing else: an idempotency fast-path for `payment`/`notifications`
  Kafka consumers and `inventory`'s reserve/release (**G17**), and — only if
  the precision is worth the extra moving part — shared rate-limit counters
  once the ingress gateway is HA (**G18**). Not used to cache `inventory`'s
  stock count: the reservation has to read the live value at write time,
  which is the entire point of G1's atomic conditional update, and a cache
  in front of that path would just reintroduce the race G1 closes. Postgres
  always keeps a unique constraint on the same idempotency key as the
  durable backstop, so a Redis restart can't silently reopen a closed door.
- **Checkout console**, one static HTML file with no framework and no build
  step, served by a minimal `nginx:alpine` off a ConfigMap. Not a frontend
  app — a deliberately thin window onto state that Grafana/Kiali/Jaeger
  don't surface: one order's status flipping live, which pod answered, and
  (from Phase 5) which canary version served the request.

## Proposed structure

```
system_scale/
├── services/
│   ├── orders/             # Spring Boot — checkout + saga orchestrator + outbox relay
│   ├── inventory/          # Spring Boot — reserve/release stock (release = compensating action)
│   ├── payment/             # Spring Boot — simulated payment processing, configurable fail rate
│   └── notifications/         # Spring Boot — Kafka consumer, simulates confirmation/cancellation sends
├── console/
│   └── index.html              # checkout console — plain HTML/JS, no framework, no build step
├── deploy/
│   ├── namespaces/         # namespace.yaml + NetworkPolicy (default-deny + allow) per ns
│   ├── data/                 # Postgres Deployment/StatefulSet + PVC, per-service schema init
│   ├── base/                   # kustomize base: Deployment/Service/HPA/PDB per service,
│   │                            # each Deployment carrying probes, resource requests/limits,
│   │                            # podAntiAffinity, and graceful-shutdown settings (G8-G12)
│   ├── overlays/
│   │   ├── dev/
│   │   └── canary/              # orders v1+v2 Deployments, subset labels
│   ├── istio/                     # Gateway, VirtualService, DestinationRule, PeerAuthentication (mTLS),
│   │                                EnvoyFilter/rate-limit config (G13), istiod+gateway HA replicas (G14)
│   ├── kafka/                       # 3-broker Kafka via Strimzi: StatefulSet + headless Service,
│   │                                  replication factor 3, per-topic partition count (G4/G7)
│   ├── cache/                         # Redis: idempotency fast-path (G17), optional rate-limit
│   │                                    counter store (G18) — its own namespace, narrow NetworkPolicy
│   ├── console/                         # nginx:alpine + ConfigMap serving console/index.html
│   └── grafana/                       # provisioned dashboards-as-code (flash-sale.json, canary.json)
├── infra/
│   ├── kind-cluster.yaml             # multi-node kind config (1 control-plane + 2 workers)
│   └── registry/                       # local image registry wired into the kind network
├── ci/
│   └── Jenkinsfile(s)                    # build/test/push/deploy + canary-promote pipeline
├── scripts/
│   ├── flash-sale.sh                       # generates the 10x load spike for Phase 6
│   └── canary-shift.sh                       # patches Istio VirtualService weights
└── docs/
    ├── 01-k8s-service-types.md                 # ClusterIP/NodePort/LoadBalancer/Headless/ExternalName
    ├── 02-namespaces-networking.md
    ├── 03-service-registry-vs-mesh.md             # explicit comparison vs. the ticket app's approach
    ├── 04-istio-setup.md
    ├── 05-canary-deployment.md
    ├── 06-cicd-jenkins.md
    └── 07-saga-and-outbox.md                          # sequence of the checkout saga, why orchestration
                                                          # over choreography, why outbox over dual-write
```

## Phased roadmap

**Phase 0 — Environment**
Get Docker Desktop running, install `kind`, `kubectl`, `helm`, `istioctl`
(and `k9s` optionally). Create a multi-node kind cluster (1 control-plane +
2 workers) so replica distribution across nodes is visible. Wire up a local
image registry on the kind network.
*Learning outcome:* explain what a kind cluster actually is (Docker
containers acting as K8s nodes) vs. a real multi-node cluster.

**Phase 1 — `orders` + `inventory`, Postgres, and every K8s Service type**
Build `orders` and `inventory` as Spring Boot REST services backed by
Postgres; `orders` calls `inventory` synchronously to reserve stock (no
saga yet — just the precondition step). `inventory`'s reservation is an
**atomic conditional update** (`UPDATE ... SET stock = stock - :qty WHERE
stock >= :qty`, or JPA `@Version` optimistic locking) rather than
read-then-write, so concurrent flash-sale requests can't both pass the stock
check and oversell (**G1**). Both `/reserve` and `/release` accept a
client-supplied idempotency key, deduped in Postgres, so a retry — including
Istio's own retry policy added in Phase 4 — can never double-reserve or
double-release (**G2**). Every Deployment gets CPU/memory requests+limits
(**G10**, required for correct HPA math and scheduler placement),
`podAntiAffinity` so replicas spread across the 2 kind workers instead of
stacking on one (**G9**), `terminationGracePeriodSeconds` + `preStop` +
Spring Boot graceful shutdown so in-flight requests finish before SIGKILL
(**G12**), and readiness/liveness probes wired to Spring Boot Actuator's
`/actuator/health/{readiness,liveness}` so Kubernetes can tell a hung pod
from a healthy one and hold traffic until warm-up finishes (**G8**). One
namespace per service, each with a default-deny NetworkPolicy plus explicit
allow rules. Deploy with plain Deployments/Services and deliberately
exercise each Service type: ClusterIP (the orders→inventory call, and each
service→Postgres call), NodePort, Headless (previewing Kafka's StatefulSet
in Phase 2), LoadBalancer (via MetalLB on kind, so a real assigned IP
replaces permanently-pending), ExternalName. Each response includes the
responding pod's hostname so a curl loop against 3 replicas visibly shows
load balancing across instances. `orders` exposes `GET /orders/{id}` for
status lookups (needed anyway to observe the saga in Phase 2), and the first
version of the **checkout console** goes up here: a static page that calls
`POST /checkout`, then polls `GET /orders/{id}` and shows the responding
pod's hostname — the same load-balancing behavior from the curl loop above,
now watchable in a browser instead of a terminal.
*Learning outcome:* for any given service, say which K8s Service type fits
and why — not just that ClusterIP "works" — and run a concurrent-request
test against `/reserve` that proves it doesn't oversell or double-reserve on
retry.

**Phase 2 — Kafka, the Outbox pattern, and the payment saga**
3-broker Kafka via Strimzi in its own `kafka` namespace, replication factor
3 / `min.insync.replicas=2` (**G7**), saga topics created with 3+ partitions
so `payment` and `notifications` can later run multiple consumer replicas in
the same consumer group instead of being capped at one (**G4** — exercised
for real in Phase 6). Build `payment` (Spring Boot) and add the outbox
mechanism to `orders`: an `outbox_events` table written in the same
transaction as order-state changes, plus a scheduled relay that publishes
unpublished rows to Kafka and marks them sent. The relay's producer runs
with `acks=all` and `enable.idempotence=true` (**G3**) — without that, the
outbox's atomicity guarantee still leaks at the very last hop. Each saga
topic gets a matching dead-letter topic so a malformed/poison message can't
wedge a partition indefinitely (**G5**). The order ID is propagated as a
Kafka message header on every hop so a single order's path through the saga
is traceable end-to-end (**G15** — Istio's sidecars trace HTTP automatically
but not Kafka, so this has to be explicit). Wire up the full saga described
above — success path (`PaymentRequested → PaymentCompleted → OrderPlaced`)
and the compensating failure path (`PaymentRequested → PaymentFailed →
inventory /release → OrderCancelled`), forcing both by toggling `payment`'s
failure rate. `notifications` consumes `OrderPlaced`/`OrderCancelled` and
logs a simulated send. Cross-namespace NetworkPolicy allows only `orders`,
`payment`, and `notifications` to reach the Kafka brokers. Write
`docs/07-saga-and-outbox.md` to explicitly state the system's consistency
model (**G16**): strong consistency only within a single service's own
transaction; everything across services — an order sitting in
`AWAITING_PAYMENT` while `payment` is unreachable — is eventually
consistent, a deliberate choice of availability over immediate consistency
during a downstream partition, recovered later via the saga's compensation
path. Consumer groups (`payment`, `notifications`, and `orders` itself
consuming the saga-result topic) use `CooperativeStickyAssignor` instead of
Kafka's default eager assignor, so a rebalance only moves the partitions
that actually changed hands instead of pausing every consumer in the group.
That still doesn't eliminate redelivery — a consumer that dies after
processing a message but before committing its offset will see that message
again on a rebalance — so `payment` and `notifications` dedupe by order ID +
event type before acting: a Redis `SETNX`-with-TTL check as the fast path,
backed by a Postgres unique constraint on the same key as the durable
fallback (**G17**, same two-tier pattern as G2, now covering the async side
too — Redis is introduced here for exactly this, not as a general cache).
*Learning outcome:* trigger both the happy path and the compensated failure
path on demand, show the outbox table and Kafka topic proving no event was
lost or duplicated across a forced pod restart mid-transaction, kill one of
the 3 Kafka brokers and show the saga keeps running uninterrupted, force a
redelivery (kill a `payment` pod mid-message before its offset commits) and
show the duplicate gets recognized and dropped instead of double-processed,
and explain why this needed a saga instead of a single DB transaction —
including which parts of the system are strongly vs. eventually consistent
and why.

**Phase 3 — Plain Ingress (pre-mesh baseline)**
Install `ingress-nginx`, expose `orders` externally via path-based Ingress.
This is the baseline contrasted against Istio's Gateway in Phase 4.
*Learning outcome:* explain what Ingress gives you and what it doesn't (no
retries/circuit-breaking/traffic-splitting without a mesh or Gateway API
extensions).

**Phase 4 — Istio service mesh + the resilience test**
Install Istio (demo profile), enable sidecar injection per namespace, verify
with `istioctl proxy-status`. Turn on strict mTLS via PeerAuthentication and
confirm pod-to-pod traffic is encrypted. Replace ingress-nginx with Istio's
Ingress Gateway. Add DestinationRule retry/timeout/circuit-breaking policies
on the `orders → inventory` call, then deliberately inject latency/errors
into `inventory` and watch the mesh contain the blast radius instead of
`orders` cascading into failure. Deploy Prometheus + Grafana + Kiali +
Jaeger, wire Spring Boot Actuator's Prometheus endpoint into the same
Prometheus, and build the **flash-sale dashboard** (request rate, replica
count, CPU, p95 latency for `orders`) — it stays flat until Phase 6
generates real traffic. Run `istiod` and the ingress gateway at 2+ replicas
(**G14**) — a single-pod mesh control plane would undercut the whole
"fault-tolerant" claim this project is making. Add a `PodDisruptionBudget`
(`minAvailable`) to `orders`, `inventory`, and `payment` (**G11**) and prove
it: drain a kind worker node and confirm at least one replica of each stays
up throughout, instead of all replicas going down together. Write
`docs/03-service-registry-vs-mesh.md` contrasting this sidecar-based setup
against the ticket app's registry+gateway pattern.
*Learning outcome:* demonstrate resilience (inventory degraded but checkout
survives), demonstrate availability through a node drain (PDB holding the
line), and explain mTLS and service-registry-vs-mesh with a working example,
not just theory.

**Phase 5 — Canary deployment: shipping v2 mid-flash-sale**
Build `orders` v2 (new discount-calculation path in checkout — a visible,
explainable behavior change that still runs the full saga). DestinationRule
subsets (v1/v2) + VirtualService weighted routing, shifted 90/10 → 50/50 →
100/0 via `scripts/canary-shift.sh`. Build the **canary dashboard** (request
rate/error rate/p95 latency split by `version` label, from Istio's Envoy
metrics) and watch it live while shifting weights. Simulate a bad v2
(injected errors) and roll back weights — the actual safety payoff, seen on
the dashboard as v2's error rate spikes while v1's stays flat. Extend the
**checkout console** to show which `orders` version answered each request —
refreshing the console during a weight shift makes the traffic split
tangible in a way percentages on a dashboard don't quite match.
*Learning outcome:* run a real canary rollout end-to-end, justify a
promote/rollback decision from dashboard data, and explain why this is safer
than a straight rolling update — especially mid-flash-sale.

**Phase 6 — The flash sale: autoscaling, back-pressure, and load shedding**
Add an HPA (CPU-based) to `orders`. Run `scripts/flash-sale.sh` (k6/hey) to
generate the 10x spike, and watch it live on the flash-sale dashboard from
Phase 4: replica count climbing on `kubectl get hpa -w`, request rate and
p95 latency on Grafana, new pods joining the Service and taking traffic
automatically. Scale `payment` and `notifications` to multiple replicas in
the same consumer group and confirm they actually split the partitions
created in Phase 2 (**G4** exercised for real — `kubectl exec` into a broker
and check partition assignment across consumers). Add an Istio local rate
limit at the ingress gateway (**G13**) so a spike that outruns HPA's
reaction time degrades on purpose — clients get `429`s instead of the whole
system falling over — and prove it by pushing load past the configured
limit. Because the ingress gateway runs 2+ replicas since Phase 4 (G14), a
local rate limit is enforced *per replica* — the real ceiling is roughly
`limit × replica count`, not the configured number (**G18**). Make the call
explicitly rather than by accident: either document that approximation as
good enough for this cluster, or swap in Envoy's Rate Limit Service backed
by the Redis instance from Phase 2 as the shared counter store for a true
global limit, and prove the difference by comparing observed request counts
at the moment `429`s start under each configuration. Optionally re-run the
Phase 5 canary shift *during* the spike, tying the whole story together.
*Learning outcome:* point to a dashboard and narrate a real autoscaling
event, not just describe HPA conceptually; explain the difference between
"scaled to absorb load" and "shed load on purpose" and when a real system
needs each; and explain why a per-replica rate limit and a global one give
different numbers, and which one was chosen here and why.

**Phase 7 — CI/CD with Jenkins**
Run Jenkins locally (container with Docker access). Jenkinsfile per
service: checkout → `mvn test` → build (multi-stage Docker, JRE base image)
→ push to the local registry → deploy via kustomize to kind. A separate
"canary promote" pipeline job patches Istio VirtualService weights as a
parameterized stage with a manual approval gate before going to 100% —
CI/CD literally driving the Phase 5 canary process.
*Learning outcome:* describe (and show) how a canary release is actually
operated from a pipeline, not run by hand.

**Phase 8 — Optional stretch**
The one gap deliberately left as a documented trade-off rather than fixed
inline: the shared single Postgres instance is still a SPOF for `orders`,
`inventory`, and `payment` at once (**G6**). Fixing it for real — a
primary + streaming replica, or splitting into per-service instances — is
meaningfully more infra than the rest of this lab needs locally, so it's
scoped here rather than in Phase 1. Also here: swap the polling outbox relay
for real CDC (Debezium + Kafka Connect tailing the Postgres WAL) — the more
"production-grade" version of Outbox, worth trying once the polling version
is solid; Argo Rollouts or Flagger for metrics-driven automated canary using
the same Grafana metrics as the promote/rollback signal; chaos testing
(`kubectl delete pod`, Istio fault injection) during a simulated flash sale;
later migrating the same manifests to a real managed cluster (EKS/GKE/DOKS)
once the mechanics are solid.

## Verification approach

Each phase is verified hands-on as it's built:
- Phase 1: curl loop against `orders`→`inventory` shows requests landing on
  different pod hostnames; each Service-type exercise gets a
  `kubectl get svc/endpoints` check plus a real successful request through
  that access path; a concurrent-request script hammering `/reserve` on a
  low-stock SKU proves it never goes negative (G1) and that resending the
  same idempotency key never double-decrements (G2); `kubectl describe pod`
  shows requests/limits set and pods spread across both worker nodes (G9/G10).
- Phase 2: forcing `payment` failure shows `inventory` stock released and
  `OrderCancelled` delivered; killing `orders`' pod mid-saga (after outbox
  write, before relay publish) and letting it restart shows the event still
  gets published exactly once from the outbox table; killing one of the 3
  Kafka broker pods mid-flow shows the saga completes anyway (G7); a
  deliberately malformed message lands on the dead-letter topic instead of
  blocking the partition (G5); killing a `payment` pod mid-message before its
  offset commits forces a rebalance and a redelivery, and the Redis/Postgres
  dedup check proves the message isn't double-processed (G17).
- Phase 3 vs 4: same request, through ingress-nginx then through Istio
  Gateway, diffing behavior.
- Phase 4: `istioctl proxy-status` shows all pods synced; injected
  `inventory` failures are contained (checkout degrades gracefully, doesn't
  cascade); flash-sale dashboard renders live data once Phase 6 runs;
  draining a kind worker node leaves at least one replica of each service up
  throughout, per its PodDisruptionBudget (G11); `kubectl get pods -n
  istio-system` shows 2+ ready replicas for `istiod` and the ingress gateway
  (G14).
- Phase 5: canary dashboard shows the v1/v2 split matching configured
  weights, and shows the error-rate spike/rollback live during the bad-v2
  simulation.
- Phase 6: `kubectl get hpa -w` and the flash-sale dashboard show replica
  count rising under `flash-sale.sh` load and falling back after; Kafka
  consumer-group partition assignment shows `payment`/`notifications`
  replicas actually splitting the load (G4); pushing load past the
  configured rate limit returns `429`s instead of cascading failures (G13);
  comparing observed request counts at the `429` threshold against the
  configured limit shows the per-replica multiplier directly, confirming
  whether the chosen approach (documented approximation vs. Redis-backed
  global limit) behaves as decided (G18).
- Phase 7: a git push triggers the Jenkins pipeline end-to-end to a running
  pod with the new image tag visible in `kubectl describe deployment`.

This is built phase by phase rather than generated all at once — each phase
ends in something runnable, watchable on a dashboard, and explainable before
moving to the next.
