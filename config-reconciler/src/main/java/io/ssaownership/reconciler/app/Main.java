package io.ssaownership.reconciler.app;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;

/**
 * Entrypoint. Configuration via environment:
 *   NAMESPACE          (default: current namespace or "default")
 *   SOURCE_CONFIGMAP   (default: "demo-source")
 *   TARGET_CONFIGMAP   (default: "demo-config")
 *   OWNED_KEY          (default: "message")
 * Runs until SIGTERM, then shuts down cleanly.
 */
public final class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws InterruptedException {
        String source = env("SOURCE_CONFIGMAP", "demo-source");
        String target = env("TARGET_CONFIGMAP", "demo-config");
        String key = env("OWNED_KEY", "message");

        try (KubernetesClient client = new KubernetesClientBuilder().build()) {
            String ns = Optional.ofNullable(env("NAMESPACE", null))
                    .orElseGet(() -> Optional.ofNullable(client.getNamespace()).orElse("default"));
            LOG.info("config-reconciler starting in namespace {}", ns);

            CountDownLatch stop = new CountDownLatch(1);
            try (ConfigReconcilerApp app = new ConfigReconcilerApp(client, ns, source, target, key)) {
                Runtime.getRuntime().addShutdownHook(new Thread(stop::countDown, "shutdown"));
                app.start();
                stop.await();                 // block until SIGTERM
                LOG.info("shutting down");
            }
        }
    }

    private static String env(String name, String def) {
        String v = System.getenv(name);
        return (v == null || v.isBlank()) ? def : v;
    }
}
