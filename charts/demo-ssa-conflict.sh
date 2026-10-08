#!/usr/bin/env bash
# End-to-end demonstration of the Helm 3 -> Helm 4 SSA ConfigMap two-writer problem and its fix,
# using the real config-reconciler application. Requires: a cluster (minikube/kind), helm3, helm4,
# docker, and the demo-app image loaded (see README). Honest about what each step shows.
set -euo pipefail
NS="${NS:-ssa-demo}"
REL="${REL:-demo}"
HELM3="${HELM3:-helm3}"
HELM4="${HELM4:-helm4}"
HERE="$(cd "$(dirname "$0")/.." && pwd)"
V1="$HERE/charts/config-reconciler-v1-two-writers"
V2="$HERE/charts/config-reconciler-v2-app-owned"

say() { printf '\n=== %s ===\n' "$1"; }

say "namespace + image"
kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f -

say "LESSON 1 — install v1 (two writers) with Helm 3 (client-side apply)"
$HELM3 -n "$NS" upgrade --install "$REL" "$V1" --wait
kubectl -n "$NS" get cm demo-config -o jsonpath='{.data.message}'; echo

say "LESSON 2 — upgrade v1 with Helm 4 server-side apply (NO --force-conflicts): expect CONFLICT"
set +e
$HELM4 -n "$NS" upgrade "$REL" "$V1" --server-side=true
echo ">>> the chart and the application both own demo-config.data.message -> SSA refuses"
set -e

say "LESSON 3 — the fix: upgrade to v2 (chart drops the key) with Helm 4 SSA, NO force"
$HELM4 -n "$NS" upgrade "$REL" "$V2" --server-side=true
kubectl -n "$NS" rollout restart deploy/"$REL"-config-reconciler
kubectl -n "$NS" rollout status  deploy/"$REL"-config-reconciler --timeout=120s
echo ">>> clean upgrade: the application is the single owner of demo-config.data.message"

say "ownership (who owns .data.message)"
kubectl -n "$NS" get cm demo-config --show-managed-fields -o json \
  | python3 -c 'import sys,json;d=json.load(sys.stdin);[print("  ",m.get("manager"),m.get("operation"),"owns-message=", "f:message" in json.dumps(m.get("fieldsV1",{}))) for m in d["metadata"].get("managedFields",[])]'

say "cleanup"
echo "  helm uninstall $REL -n $NS ; kubectl delete ns $NS"
