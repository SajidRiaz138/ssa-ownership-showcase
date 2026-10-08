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
 * The runnable application: it is the RUNTIME SOURCE OF TRUTH for the field
 * {@code <configMap>.data.<key>} and enforces it through reconciliation — the "application owns it
 * at runtime, not the chart" resolution to the SSA two-writer problem.
 *
 * <p><b>Note on "ownership".</b> This is operational enforcement, not named server-side-apply
 * field ownership: {@link KubeConfigMapStore} uses ordinary resourceVersion-guarded updates, so
 * the application does not register an SSA field manager in {@code managedFields} for this key.
 * The fix here is organisational (the chart stops writing the key; the app enforces it). Named SSA
 * field-manager behaviour — co-ownership conflicts and an ownership handoff — is demonstrated
 * separately by the SchemaBinding operator scenario, not by this ConfigMap path.
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
    private final ScheduledExecutorService worker;
    private final ScheduledExecutorService recoveryExec;

    private enum LifeState { NEW, STARTED, CLOSED }
    private final java.util.concurrent.atomic.AtomicReference<LifeState> lifeState =
            new java.util.concurrent.atomic.AtomicReference<>(LifeState.NEW);

    public ConfigReconcilerApp(KubernetesClient client, String namespace,
                               String sourceConfigMap, String targetConfigMap, String key) {
        this(client, namespace, sourceConfigMap, targetConfigMap, key,
                Duration.ofSeconds(2), Duration.ofMinutes(5));
    }

    public ConfigReconcilerApp(KubernetesClient client, String namespace,
                               String sourceConfigMap, String targetConfigMap, String key,
                               Duration retryDelay, Duration resyncInterval) {
        this.client = Objects.requireNonNull(client, "client");
        this.namespace = requireNonBlank(namespace, "namespace");
        this.sourceConfigMap = requireNonBlank(sourceConfigMap, "sourceConfigMap");
        this.targetConfigMap = requireNonBlank(targetConfigMap, "targetConfigMap");
        this.key = requireNonBlank(key, "key");

        // Create executors as LOCALS and only publish them (and the composed dependencies) after
        // all construction succeeds, shutting them down if any later step throws — so a failed
        // construction leaks no daemon executors.
        ScheduledExecutorService w = Executors.newSingleThreadScheduledExecutor(daemon("config-reconciler-worker"));
        ScheduledExecutorService rec = Executors.newSingleThreadScheduledExecutor(daemon("config-reconciler-recovery"));
        try {
            ConfigMapStore store = new KubeConfigMapStore(client, namespace);
            Reconciler reconciler = new Reconciler("config-reconciler", targetConfigMap, key,
                    WriteStrategy.UPSERT, store, this::deriveFromSource);
            this.coordinator = new ReconcileCoordinator(reconciler, w, retryDelay, resyncInterval);
            this.lifecycle = new WatchLifecycle("source-watch", this::watchSource,
                    new ExecutorRecoveryScheduler(rec));
        } catch (RuntimeException | Error constructionFailure) {
            w.shutdownNow();
            rec.shutdownNow();
            throw constructionFailure;
        }
        this.worker = w;
        this.recoveryExec = rec;
    }

    private static java.util.concurrent.ThreadFactory daemon(String name) {
        return r -> { Thread t = new Thread(r, name); t.setDaemon(true); return t; };
    }

    private static String requireNonBlank(String v, String what) {
        Objects.requireNonNull(v, what);
        if (v.isBlank()) throw new IllegalArgumentException(what + " must not be blank");
        return v;
    }

    /**
     * Derive the value to enforce from the SOURCE ConfigMap's {@code desired} key.
     *
     * <p><b>Missing-source policy (deliberate).</b> If the source ConfigMap is absent, has no data,
     * or lacks the {@code desired} key, this throws — the reconcile pass becomes
     * {@link Reconciler.Outcome#FAILED}, which performs NO write and therefore PRESERVES the
     * current target value. This is intentional: an accidental source deletion must NOT cause the
     * target to be overwritten with a fabricated default. (If a default were desired for a truly
     * absent source, that would be a different, explicit policy.)
     */
    private String deriveFromSource() {
        ConfigMap src = client.configMaps().inNamespace(namespace).withName(sourceConfigMap).get();
        String desired = (src != null && src.getData() != null) ? src.getData().get("desired") : null;
        if (desired == null) {
            throw new IllegalStateException("source " + sourceConfigMap
                    + " missing or has no 'desired' key; preserving target (reconcile FAILED)");
        }
        return desired;
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

    /** Start: begin periodic resync and install the watch (which triggers the initial reconcile).
     *  One-time: a second {@code start()} (or {@code start()} after {@code close()}) is rejected. */
    public void start() {
        if (!lifeState.compareAndSet(LifeState.NEW, LifeState.STARTED)) {
            throw new IllegalStateException("start() is one-time; current state=" + lifeState.get());
        }
        LOG.info("starting; source={} target={}.{}", sourceConfigMap, targetConfigMap, key);
        try {
            coordinator.startPeriodicResync();
            lifecycle.registerWatch();
        } catch (RuntimeException | Error startupFailure) {
            close();        // roll back: do not leave the app stuck in STARTED
            throw startupFailure;
        }
    }

    /**
     * Bounded, coordinated, IDEMPOTENT shutdown. Deactivate the lifecycle (no further installs),
     * close the coordinator (terminal; cancels retry/resync), then shut the executors down and
     * AWAIT their termination within a budget. {@code shutdownNow()} only requests interruption;
     * termination is what we actually wait for. The caller keeps the Kubernetes client open until
     * this returns. A second call is a no-op.
     */
    @Override
    public void close() {
        if (lifeState.getAndSet(LifeState.CLOSED) == LifeState.CLOSED) {
            return;     // idempotent
        }
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
