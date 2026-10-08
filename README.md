# SSA Ownership Showcase — Helm 3 → Helm 4 / Server-Side Apply field ownership

A reference implementation of the field-ownership problems a real Helm 3 → Helm 4 migration
surfaces, and their correct fixes — demonstrated with runnable Java controllers and tests against
a real Kubernetes API server, not slides.

**Author:** Sajid Riaz · **License:** Apache-2.0 · **Java 21 · Maven · fabric8**

Helm 4 defaults to **server-side apply (SSA)**, which enforces per-field ownership. Fields that a
chart and an application both wrote under Helm 3's client-side apply now **conflict**. This repo
reproduces the two canonical cases and shows the fixes:

1. **Chart and app co-own a ConfigMap field** → fix: the chart stops writing it; the application
   owns it at runtime (an enforcing reconciler).
2. **An operator co-owns a CRD field the chart owns** (`spec.schemaVersion`) → fix: the operator
   records observed state in `status`, and performs an **ownership handoff** on upgrade. This is
   the class of conflict a chart cannot fix alone.

## What this demonstrates (and where the evidence is)

| Capability | Where | Verified by |
|---|---|---|
| Single-lock watch **lifecycle** with a reservation protocol (concurrent registration, close-before-install, stale-callback, shutdown races, non-recursive scheduled recovery, backoff) | `reconciler-core/WatchLifecycle` | 15 unit tests + 2 mutation-demonstrated regressions |
| **Enforcement vs seeding** reconcile engine (observed-destination comparison; UPSERT vs PUT_IF_ABSENT) | `reconciler-core/Reconciler` | 13 unit tests |
| Cancellable delayed **recovery scheduler** (executor ownership, rejection→DEGRADED) | `reconciler-core/ExecutorRecoveryScheduler` | 5 integration tests (+ cancellation mutation check) |
| **fabric8 ConfigMap adapter** with concurrency-safe conditional writes | `config-reconciler/KubeConfigMapStore` | 6 **live-API** tests (incl. the deterministic conditional-update race) |
| **Runnable controller** that owns a ConfigMap field end-to-end | `config-reconciler/ConfigReconcilerApp` + `Main` | 1 **live-API** end-to-end test; fat jar + Dockerfile |
| **Operator field-ownership**: broken (co-owns spec) vs fixed (status-only) + ownership handoff | `schemasync-operator` | 3 **live SSA** tests on real managedFields |
| Helm charts v1 (two-writers) / v2 (app-owned) / v3 (operator-fixed) + demo | `charts/` | `helm lint` + rendered-invariant check in CI |

## Project status

**Verified** (JDK 21 / Maven 3.9.10; integration tests on minikube k8s 1.30 and in CI on kind):
- All unit tests (`reconciler-core`): lifecycle + reconciler.
- All integration tests: scheduler, the fabric8 adapter's live conditional-update race, the
  end-to-end app reconcile, and the live SSA operator ownership + handoff.
- Two mutation checks: the authorization-gap test and the cancellation test each fail against the
  corresponding injected bug (evidence they test what they claim).

**Scope / honest limits:**
- Correctness is established by the listed tests under the documented conditions and Kubernetes
  version — not asserted as universal.
- A few lifecycle edge transitions (scheduling handle-publication races, shutdown-during-factory,
  replacement-fails-while-old-healthy) are source-correct but covered structurally, not each by a
  dedicated test — see ADR-0002's "known gaps".

## Build & test

Requires JDK 21 and (for integration tests) a reachable cluster (minikube/kind). Self-bootstrapping
Maven wrapper is committed.

```
./mvnw verify                 # unit + integration (ITs auto-skip if no cluster)
./mvnw -DskipITs=true test    # unit tests only
./mvnw clean verify -Plive-it # MANDATORY integration run: fails (not skips) if no cluster
```

**Full step-by-step guide to stand up a cluster, verify, deploy, and run the SSA demos:
[`docs/RUNNING.md`](docs/RUNNING.md).**

## Architecture & decisions
- `docs/ARCHITECTURE.md` — components, the ownership model, runtime data flow, lifecycle & shutdown.
- `docs/SCENARIOS.md` — every scenario the system handles, mapped to its verifying test + evidence class.
- `docs/adr/ADR-0001` reconciliation contract · `ADR-0002` lifecycle & recovery ·
  `ADR-0003` fabric8 adapter · `ADR-0004` operator field ownership · `ADR-0005` runtime coordination.
- `docs/verification-record.md` — corrected source / reviewed coverage / executed verification.
- `docs/mutation-evidence.md` — the executed old-fails/new-passes runs for the three DEMO regressions.

## The runnable demo
```
# build + load the image (minikube), then:
charts/demo-ssa-conflict.sh    # install v1, see the SSA conflict, upgrade to v2 clean
```

## Layout
```
reconciler-core/        lifecycle + reconcile engine + scheduler (+ tests)
config-reconciler/      fabric8 adapter + runnable app + Main + Dockerfile (+ live-API ITs)
schemasync-operator/    SchemaBinding CRD + broken/fixed operator + handoff (+ live SSA ITs)
charts/                 v1 two-writers / v2 app-owned / v3 operator-fixed + demo script
docs/adr/               ADR-0001..0004
.github/workflows/ci.yml  build + unit (always) + kind integration + helm lint
```
