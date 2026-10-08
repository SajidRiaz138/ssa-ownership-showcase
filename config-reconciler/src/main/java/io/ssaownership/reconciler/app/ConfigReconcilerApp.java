package io.ssaownership.reconciler.app;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import io.ssaownership.reconciler.ConfigMapStore;
import io.ssaownership.reconciler.ExecutorRecoveryScheduler;
import io.ssaownership.reconciler.Reconciler;
import io.ssaownership.reconciler.WatchFactory;
import io.ssaownership.reconciler.WatchLifecycle;
import io.ssaownership.reconciler.WriteStrategy;
import io.ssaownership.reconciler.k8s.KubeConfigMapStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * The runnable application: it OWNS the field {@code <configMap>.data.<key>} at runtime,
 * demonstrating the "application owns it, not the chart" resolution to the SSA two-writer problem.
 *
 * Composition:
 * <ul>
 *   <li>{@link KubeConfigMapStore} — the fabric8 adapter (reads/conditional-writes the field).</li>
 *   <li>{@link Reconciler} — observed-destination enforcement (UPSERT) of the derived value.</li>
 *   <li>{@link WatchLifecycle} — manages a watch on the SOURCE ConfigMap; a source change triggers
 *       a reconcile. Watch failures recover via {@link ExecutorRecoveryScheduler}.</li>
 * </ul>
 * Reconcile passes run on a single-thread worker (NOT on the watch callback thread), and are
 * coalesced naturally because the Reconciler compares desired vs observed and skips when in sync.
 */
public final class ConfigReconcilerApp implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigReconcilerApp.class);

    private final KubernetesClient client;
    private final String namespace;
    private final String sourceConfigMap;   // the watched input (e.g. desired-state)
    private final String targetConfigMap;   // the field we own lives here
    private final String key;
    private final Reconciler reconciler;
    private final WatchLifecycle lifecycle;
    private final ExecutorService worker = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "config-reconciler-worker"); t.setDaemon(true); return t;
    });
    private final ScheduledExecutorService recoveryExec = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "config-reconciler-recovery"); t.setDaemon(true); return t;
    });

    public ConfigReconcilerApp(KubernetesClient client, String namespace,
                               String sourceConfigMap, String targetConfigMap, String key) {
        this.client = Objects.requireNonNull(client);
        this.namespace = Objects.requireNonNull(namespace);
        this.sourceConfigMap = Objects.requireNonNull(sourceConfigMap);
        this.targetConfigMap = Objects.requireNonNull(targetConfigMap);
        this.key = Objects.requireNonNull(key);

        ConfigMapStore store = new KubeConfigMapStore(client, namespace);
        // Derive the owned value from the SOURCE ConfigMap's own data (its "desired" key), or a
        // default if the source has nothing yet. In a real app this would be a richer derivation.
        this.reconciler = new Reconciler("config-reconciler", targetConfigMap, key,
                WriteStrategy.UPSERT, store, this::deriveFromSource);

        WatchFactory factory = onClosed -> watchSource(onClosed);
        this.lifecycle = new WatchLifecycle("source-watch", factory,
                new ExecutorRecoveryScheduler(recoveryExec));
    }

    /** Derive the value the app enforces into the target, from the source ConfigMap. */
    private String deriveFromSource() {
        ConfigMap src = client.configMaps().inNamespace(namespace).withName(sourceConfigMap).get();
        String desired = (src != null && src.getData() != null) ? src.getData().get("desired") : null;
        return desired != null ? desired : "derived-default";
    }

    /** Create a watch on the source ConfigMap; each event triggers a reconcile on the worker. */
    private Watch watchSource(Runnable onClosed) {
        return client.configMaps().inNamespace(namespace).withName(sourceConfigMap)
                .watch(new Watcher<>() {
                    @Override public void eventReceived(Action action, ConfigMap resource) {
                        LOG.info("source event {}; scheduling reconcile", action);
                        worker.submit(ConfigReconcilerApp.this::reconcileSafely);
                    }
                    @Override public void onClose(WatcherException cause) {
                        LOG.info("source watch closed: {}", cause == null ? "(graceful)" : cause.getMessage());
                        onClosed.run();     // hand back to the lifecycle (recovery)
                    }
                });
    }

    private void reconcileSafely() {
        try {
            Reconciler.Outcome outcome = reconciler.reconcile();
            LOG.info("reconcile outcome: {}", outcome);
        } catch (RuntimeException e) {
            LOG.error("reconcile pass threw unexpectedly", e);
        }
    }

    /** Start: install the watch and do an initial reconcile (startup seeding/enforcement). */
    public void start() {
        LOG.info("starting; source={} target={}.{}", sourceConfigMap, targetConfigMap, key);
        lifecycle.registerWatch();
        worker.submit(this::reconcileSafely);   // initial pass
    }

    @Override
    public void close() {
        lifecycle.deactivate();
        worker.shutdownNow();
        recoveryExec.shutdownNow();
    }

    // accessors for tests
    WatchLifecycle lifecycle() { return lifecycle; }
    void reconcileNow() { reconcileSafely(); }
}
