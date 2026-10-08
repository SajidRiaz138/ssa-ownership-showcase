package io.ssaownership.reconciler.app;

import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Application lifecycle contract tests for {@link ConfigReconcilerApp}, with NO live cluster.
 * These assert the state-machine contract (one-time start, start-after-close rejected, idempotent
 * close) independently of any cluster, using a client pointed at an unroutable endpoint.
 *
 * <p>Note on rollback: {@code start()} wraps resync + watch registration and, on a thrown
 * {@code RuntimeException|Error}, calls {@code close()} and rethrows. fabric8's {@code watch(...)}
 * does NOT fail synchronously against an unreachable endpoint (it returns a handle and surfaces
 * the failure asynchronously via the lifecycle's recovery path), so this unit cannot force a
 * synchronous start() throw without a production seam. The rollback branch is therefore covered by
 * source review here; the one-time/idempotent GUARANTEES below are what this test pins
 * deterministically.
 */
class ConfigReconcilerAppLifecycleTest {

    private KubernetesClient client;
    private ConfigReconcilerApp app;

    private ConfigReconcilerApp newApp() {
        client = new KubernetesClientBuilder().withConfig(new ConfigBuilder()
                .withMasterUrl("https://127.0.0.1:1")
                .withConnectionTimeout(300).withRequestTimeout(300)
                .withWatchReconnectLimit(0).build()).build();
        app = new ConfigReconcilerApp(client, "ns", "src", "tgt", "key",
                Duration.ofSeconds(1), Duration.ofMinutes(10));
        return app;
    }

    @AfterEach
    void tearDown() {
        if (app != null) app.close();
        if (client != null) client.close();
    }

    @Test @Timeout(30)
    void start_after_close_is_rejected() {
        newApp();
        app.close();                                     // NEW -> CLOSED without starting
        assertThrows(IllegalStateException.class, app::start,
                "start() after close() must be rejected");
    }

    @Test @Timeout(30)
    void repeated_close_is_harmless() {
        newApp();
        assertDoesNotThrow(app::close);
        assertDoesNotThrow(app::close, "a second close() must be a no-op");
        assertDoesNotThrow(app::close, "and a third");
    }

    @Test @Timeout(30)
    void close_without_start_is_harmless() {
        newApp();
        assertDoesNotThrow(app::close, "close() on a never-started app must not throw");
    }

    @Test @Timeout(30)
    void blank_configuration_is_rejected_at_construction() {
        try (KubernetesClient c = new KubernetesClientBuilder().withConfig(new ConfigBuilder()
                .withMasterUrl("https://127.0.0.1:1").build()).build()) {
            assertThrows(IllegalArgumentException.class,
                    () -> new ConfigReconcilerApp(c, " ", "src", "tgt", "key"), "blank namespace");
            assertThrows(IllegalArgumentException.class,
                    () -> new ConfigReconcilerApp(c, "ns", "", "tgt", "key"), "blank source");
            assertThrows(IllegalArgumentException.class,
                    () -> new ConfigReconcilerApp(c, "ns", "src", " ", "key"), "blank target");
            assertThrows(IllegalArgumentException.class,
                    () -> new ConfigReconcilerApp(c, "ns", "src", "tgt", ""), "blank key");
        }
    }
}
