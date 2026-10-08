# Running & verifying

A reviewer's guide: three paths, from "prove it works in two minutes" to "deploy it on a cluster
and watch it enforce a field." Every command here matches an asset in the repo (chart image
`config-reconciler:local`, deployment `<release>-config-reconciler`, CRD
`schemasync-operator/src/main/resources/crd/schemabinding-crd.yaml`, env vars
`NAMESPACE / SOURCE_CONFIGMAP / TARGET_CONFIGMAP / OWNED_KEY`).

## Prerequisites

| Tool | Version used | Needed for |
|---|---|---|
| JDK | 21 (Temurin) | build + tests |
| Maven | 3.9.10 (via `./mvnw`, no install needed) | build + tests |
| A cluster | minikube v1.33 / kind — Kubernetes **1.30** | integration tests, deploy, demos |
| kubectl | matching the cluster | deploy, inspect |
| docker | any recent | build the image |
| helm 3 **and** helm 4 | side by side (`helm3`, `helm4` on PATH) | the Helm 3→4 SSA demo only |

The Maven wrapper is committed, so `./mvnw` bootstraps the exact Maven version — nothing to install
but a JDK and a cluster.

Point `KUBECONFIG` at your cluster (minikube does this automatically):
```
export KUBECONFIG=$HOME/.kube/config
minikube start --kubernetes-version=v1.30.0     # or: kind create cluster --image kindest/node:v1.30.0
kubectl get nodes                               # sanity
```

---

## Path A — fastest verification (recommended for reviewers)

Run the whole suite, including the live-cluster integration tests, and FAIL (not skip) if the
cluster is unreachable:

```
./mvnw clean verify -Plive-it
```

Expected: **BUILD SUCCESS, 85 tests, 0 skipped** (minikube k8s 1.30). This exercises everything —
the reconciler/lifecycle/coordinator unit tests, the real fabric8 conditional-write race, the
end-to-end app reconcile (initial / source-change / autonomous resync drift repair / missing-source
preservation), and the live SSA operator conflict + ownership handoff with `managedFields`
assertions.

Variants:
```
./mvnw -DskipITs=true test        # unit tests only (no cluster)
./mvnw verify                     # unit + ITs, but ITs self-SKIP if no cluster (local convenience)
```

Reproduce the three demonstrated regressions (old-fails / new-passes) — see
`docs/mutation-evidence.md` for the exact edits and captured failures.

The per-test → scenario map is `docs/SCENARIOS.md`; the executed build record is
`docs/verification-record.md`.

---

## Path B — deploy the config-reconciler app and watch it enforce a field

This runs the real application in-cluster and shows it owning `demo-config.data.message` at runtime.

### 1. Build the image into the cluster

The chart expects `config-reconciler:local` with `pullPolicy: IfNotPresent`, so the image must be
present in the cluster's docker daemon (not a remote registry).

minikube:
```
eval "$(minikube docker-env)"                       # build straight into minikube's daemon
docker build -t config-reconciler:local -f config-reconciler/Dockerfile .
eval "$(minikube docker-env -u)"                    # restore your shell
```
kind:
```
docker build -t config-reconciler:local -f config-reconciler/Dockerfile .
kind load docker-image config-reconciler:local
```
(The Dockerfile is multi-stage: it builds the shaded jar with JDK 21 and runs it on a JRE as a
non-root user. The build context must be the repo ROOT so the module can resolve `reconciler-core`.)

### 2. Install the chart (v2 = app-owned, the correct model)

```
NS=ssa-app
kubectl create namespace "$NS"
helm install demo charts/config-reconciler-v2-app-owned -n "$NS"
kubectl -n "$NS" rollout status deploy/demo-config-reconciler --timeout=120s
```
The chart renders a source ConfigMap (`demo-source`, `data.desired=v1`), an empty target
ConfigMap (`demo-config`), RBAC, and the Deployment.

### 3. Verify it enforces the derived value

```
# the app derives demo-config.data.message from demo-source.data.desired and enforces it:
kubectl -n "$NS" get cm demo-config -o jsonpath='{.data.message}'; echo      # -> v1

# change the source; the watch triggers a reconcile:
kubectl -n "$NS" patch cm demo-source --type merge -p '{"data":{"desired":"v2"}}'
sleep 2
kubectl -n "$NS" get cm demo-config -o jsonpath='{.data.message}'; echo      # -> v2

# tamper with the target directly; periodic resync (or a source event) repairs it:
kubectl -n "$NS" patch cm demo-config --type merge -p '{"data":{"message":"tampered"}}'
sleep 2
kubectl -n "$NS" get cm demo-config -o jsonpath='{.data.message}'; echo      # -> back to v2

kubectl -n "$NS" logs deploy/demo-config-reconciler | tail -20               # reconcile outcomes
```

### 4. Clean up
```
helm uninstall demo -n "$NS"; kubectl delete ns "$NS"
```

> Note on "ownership": this path shows the application as the **runtime source of truth** for the
> key (ordinary resourceVersion-guarded writes). It is NOT a named SSA field manager — that is what
> the operator demo (Path C) demonstrates.

---

## Path C — the SSA ownership demonstrations

### C.1 Helm 3 → Helm 4 two-writer conflict and its fix (scripted)

Requires `helm3` and `helm4` on PATH and the image from Path B step 1.
```
NS=ssa-demo ./charts/demo-ssa-conflict.sh
```
It installs v1 (chart AND app both write `.data.message`) with Helm 3, then upgrades with Helm 4
server-side apply and shows the **conflict**, then upgrades to v2 (chart drops the key) and shows a
clean upgrade — printing which manager owns `.data.message` via `--show-managed-fields`.

### C.2 Operator co-ownership conflict + ownership handoff (automated test)

The operator scenario is fully automated as a live test (no manual steps). It installs the CRD,
has the "chart" SSA-apply `spec.schemaVersion`, the broken operator co-own it (→ real 409), the
fixed operator write `/status` only, and the migration handoff drop the stale claim while
preserving the chart's value — all asserted against real `managedFields`:
```
./mvnw -pl schemasync-operator -am verify -Plive-it
```
To explore it by hand, the CRD is `schemasync-operator/src/main/resources/crd/schemabinding-crd.yaml`
(`kubectl apply -f` it, then `kubectl get schemabindings -o yaml --show-managed-fields`).

> The operator tests install a **cluster-scoped CRD** and do not remove it. Run them on a
> disposable/dedicated cluster.

---

## Running the app outside a chart (local jar)

```
./mvnw -pl config-reconciler -am -DskipTests package
NAMESPACE=default SOURCE_CONFIGMAP=demo-source TARGET_CONFIGMAP=demo-config OWNED_KEY=message \
  java -jar config-reconciler/target/config-reconciler-*.jar
```
It uses your current kubeconfig context. SIGTERM (Ctrl-C) triggers the bounded, coordinated
shutdown.

## Troubleshooting

| Symptom | Likely cause / fix |
|---|---|
| ITs are **skipped** | No reachable cluster. Use `-Plive-it` to make that a FAILURE, and check `kubectl get nodes`. |
| `ImagePullBackOff` on the deployment | Image not in the cluster daemon. Re-run Path B step 1 (`minikube docker-env` / `kind load`). |
| Pod `CrashLoopBackOff` with RBAC errors | The chart's Role grants get/list/watch/update/patch on configmaps in its namespace; confirm the ServiceAccount is bound. |
| `demo-config.data.message` stays empty | `demo-source.data.desired` is missing — by design the app FAILS and PRESERVES the target rather than writing a default. Set `desired`. |
| Operator test fails at CRD readiness | The test waits (bounded) for `Established=True`; a very slow apiserver can exceed it. Re-run; ensure the identity can install CRDs. |
