package io.ssaownership.reconciler.app;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Entrypoint. Configuration via environment:
 *   NAMESPACE          (default: current namespace or "default")
 *   SOURCE_CONFIGMAP   (default: "demo-source")
 *   TARGET_CONFIGMAP   (default: "demo-config")
 *   OWNED_KEY          (default: "message")
 *
 * <p>Runs until SIGTERM, then shuts down with a bounded, coordinated sequence. The subtlety: the
 * JVM waits for shutdown HOOKS to finish, not for arbitrary work on the main thread. So the hook
 * must itself drive (or wait for) cleanup; a hook that merely releases a latch can return — and
 * let the JVM exit — before the main thread finishes closing the app and client. Here the hook
 * signals the main thread AND waits, with a bound, for cleanup to complete.
 */
public final class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);
    private static final long SHUTDOWN_BUDGET_SECONDS = 25;

    public static void main(String[] args) throws InterruptedException {
        String source = env("SOURCE_CONFIGMAP", "demo-source");
        String target = env("TARGET_CONFIGMAP", "demo-config");
        String key = env("OWNED_KEY", "message");

        KubernetesClient client = new KubernetesClientBuilder().build();
        try {
            String ns = Optional.ofNullable(env("NAMESPACE", null))
                    .orElseGet(() -> Optional.ofNullable(client.getNamespace()).orElse("default"));
            LOG.info("config-reconciler starting in namespace {}", ns);

            // Ownership of `app` begins the instant construction succeeds: EVERY later path —
            // including addShutdownHook() throwing (shutdown already in progress, or a security
            // manager) — must close it. So the app is owned by its own try/finally that wraps hook
            // registration and start.
            ConfigReconcilerApp app = new ConfigReconcilerApp(client, ns, source, target, key);
            CountDownLatch stop = new CountDownLatch(1);        // "please begin shutdown"
            CountDownLatch cleanupDone = new CountDownLatch(1); // "app + client cleanup finished"
            try {
                Thread hook = new Thread(() -> {
                    stop.countDown();                           // ask main to begin cleanup
                    try {
                        if (!cleanupDone.await(SHUTDOWN_BUDGET_SECONDS, TimeUnit.SECONDS)) {
                            LOG.warn("shutdown budget elapsed before cleanup completed");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }, "shutdown");
                Runtime.getRuntime().addShutdownHook(hook);     // if THIS throws, finally still closes app

                app.start();
                stop.await();                                   // block until SIGTERM (hook) or error
                LOG.info("shutting down");
            } finally {
                // Nested cleanup so a client.close() failure cannot skip cleanupDone (which would
                // make the hook wait out its full budget). Order: app -> client -> release hook.
                try {
                    app.close();                                // bounded; awaits worker/recovery termination
                } finally {
                    try {
                        client.close();
                    } finally {
                        cleanupDone.countDown();
                    }
                }
            }
        } finally {
            // If app construction threw, the inner block never ran; ensure the client is closed.
            // When the inner block ran, client.close() already happened — close() is idempotent.
            client.close();
        }
    }

    private static String env(String name, String def) {
        String v = System.getenv(name);
        return (v == null || v.isBlank()) ? def : v;
    }
}
