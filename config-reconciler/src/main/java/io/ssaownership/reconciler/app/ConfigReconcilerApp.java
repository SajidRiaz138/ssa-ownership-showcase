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

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The runnable application: it OWNS the field {@code <configMap>.data.<key>} at runtime,
 * demonstrating the "application owns it, not the chart" resolution to the SSA two-writer problem.
 *
 * <h2>Composition</h2>
 * <ul>
 *   <li>{@link KubeConfigMapStore} — the fabric8 adapter (reads/conditional-writes the field).</li>
 *   <li>{@link Reconciler} — observed-destination enforcement (UPSERT) of the derived value.</li>
 *   <li>{@link ReconcileCoordinator} — runtime coordination: coalesces triggers, retries FAILED
 *       passes, periodic resync, terminal closed state. (The Reconciler compares desired vs
 *       observed and skips redundant writes; that is NOT coalescing — the coordinator provides
 *       coalescing and retry.)</li>
 *   <li>{@link WatchLifecycle} — manages a watch on the SOURCE ConfigMap; source events and each
 *       (re)install trigger a reconcile. Watch failures recover via
 *       {@link ExecutorRecoveryScheduler}.</li>
 * </ul>
 * All reconcile work runs on a single-thread worker (NOT on the watch callback thread); triggers
 * are routed through the coordinator, which is shutdown-aware so a late event cannot throw an
 * executor rejection back into a fabric8 callback.
 */
public final class ConfigReconcilerApp implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigReconcilerApp.class);

    private final KubernetesClient client;
    private final String namespace;
    private final String sourceConfigMap;
    private final String targetConfigMap;
    private final String key;
    private final ReconcileCoordinator coordinator;
    private final WatchLifecycle lifecycle;
    private final ScheduledExecutorService worker = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "config-reconciler-worker"); t.setDaemon(true); return t;
    });
    private final ScheduledExecutorService recoveryExec = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "config-reconciler-recovery"); t.setDaemon(true); return t;
    });

    public ConfigReconcilerApp(KubernetesClient client, String namespace,
                               String sourceConfigMap, String targetConfigMap, String key) {
        this(client, namespace, sourceConfigMap, targetConfigMap, key,
                Duration.ofSeconds(2), Duration.ofMinutes(5));
    }

    public ConfigReconcilerApp(KubernetesClient client, String namespace,
                               String sourceConfigMap, String targetConfigMap, String key,
                               Duration retryDelay, Duration resyncInterval) {
        this.client = Objects.requireNonNull(client);
        this.namespace = Objects.requireNonNull(namespace);
        this.sourceConfigMap = Objects.requireNonNull(sourceConfigMap);
        this.targetConfigMap = Objects.requireNonNull(targetConfigMap);
        this.key = Objects.requireNonNull(key);

        ConfigMapStore store = new KubeConfigMapStore(client, namespace);
        Reconciler reconciler = new Reconciler("config-reconciler", targetConfigMap, key,
                WriteStrategy.UPSERT, store, this::deriveFromSource);
        this.coordinator = new ReconcileCoordinator(reconciler, worker, retryDelay, resyncInterval);

        WatchFactory factory = this::watchSource;
        this.lifecycle = new WatchLifecycle("source-watch", factory,
                new ExecutorRecoveryScheduler(recoveryExec));
    }

    private String deriveFromSource() {
        ConfigMap src = client.configMaps().inNamespace(namespace).withName(sourceConfigMap).get();
        String desired = (src != null && src.getData() != null) ? src.getData().get("desired") : null;
        return desired != null ? desired : "derived-default";
    }

    /**
     * Create a watch on the source ConfigMap. If any post-creation setup throws, the newly created
     * watch is CLOSED before rethrowing — otherwise the lifecycle, which never received the handle,
     * could not close it and it would leak. (Reproduced by the shutdown-during-create test.)
     * Both event delivery and (re)install route through the shutdown-aware coordinator.
     */
    private Watch watchSource(Runnable onClosed) {
        return createThenSetup(
                () -> client.configMaps().inNamespace(namespace).withName(sourceConfigMap)
                        .watch(new Watcher<>() {
                            @Override public void eventReceived(Action action, ConfigMap resource) {
                                LOG.info("source event {}; triggering reconcile", action);
                                coordinator.trigger();   // shutdown-aware; never throws back into fabric8
                            }
                            @Override public void onClose(WatcherException cause) {
                                LOG.info("source watch closed: {}", cause == null ? "(graceful)" : cause.getMessage());
                                onClosed.run();          // hand back to the lifecycle (recovery)
                            }
                        }),
                // reconcile-on-(re)install: resync the derived field for anything missed while
                // disconnected (recovery uses this same path).
                coordinator::trigger);
    }

    /**
     * Create a watch, then run post-creation {@code setup}. If setup throws, the newly created
     * watch is CLOSED (suppressing any close error) before rethrowing — otherwise the lifecycle,
     * which never received the handle, could not close it and it would leak. Package-private and
     * static so the leak-safety can be unit-tested with a fake watch and a throwing setup.
     */
    static Watch createThenSetup(java.util.function.Supplier<Watch> create, Runnable setup) {
        Watch watch = create.get();
        try {
            setup.run();
            return watch;
        } catch (RuntimeException | Error setupFailure) {
            try {
                watch.close();
            } catch (RuntimeException closeFailure) {
                setupFailure.addSuppressed(closeFailure);
            }
            throw setupFailure;
        }
    }

    /** Start: begin periodic resync and install the watch (which triggers the initial reconcile). */
    public void start() {
        LOG.info("starting; source={} target={}.{}", sourceConfigMap, targetConfigMap, key);
        coordinator.startPeriodicResync();
        lifecycle.registerWatch();
    }

    /**
     * Bounded, coordinated shutdown. Deactivate the lifecycle (no further installs), close the
     * coordinator (terminal; no new drains), then shut the executors down and AWAIT their
     * termination within a budget. {@code shutdownNow()} only requests interruption; termination
     * is what we actually wait for. The caller keeps the Kubernetes client open until this returns.
     */
    @Override
    public void close() {
        lifecycle.deactivate();
        coordinator.close();
        worker.shutdownNow();
        recoveryExec.shutdownNow();
        awaitTermination(worker, "worker");
        awaitTermination(recoveryExec, "recovery");
    }

    private void awaitTermination(ScheduledExecutorService ex, String name) {
        try {
            if (!ex.awaitTermination(10, TimeUnit.SECONDS)) {
                LOG.warn("{} executor did not terminate within the shutdown budget", name);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // accessors for tests
    WatchLifecycle lifecycle() { return lifecycle; }
    void triggerNow() { coordinator.trigger(); }
}
