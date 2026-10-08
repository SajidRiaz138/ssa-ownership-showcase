package io.ssaownership.reconciler.app;

import io.fabric8.kubernetes.client.Watch;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link ConfigReconcilerApp#createThenSetup}: the watch-acquisition failure path.
 * No cluster required. This reproduces the reviewer's leak scenario deterministically — post-
 * creation setup (e.g. a rejected reconcile submission during shutdown) throws, and the newly
 * created watch must be closed rather than leaked, since the lifecycle never received the handle.
 */
class ConfigReconcilerAppWatchLeakTest {

    static final class FakeWatch implements Watch {
        final AtomicInteger closes = new AtomicInteger();
        boolean throwOnClose = false;
        @Override public void close() {
            closes.incrementAndGet();
            if (throwOnClose) throw new IllegalStateException("close failed");
        }
    }

    @Test
    void setup_failure_closes_the_created_watch_and_rethrows() {
        FakeWatch w = new FakeWatch();
        RuntimeException boom = new RuntimeException("setup failed (e.g. executor rejected)");
        RuntimeException thrown = assertThrows(RuntimeException.class,
                () -> ConfigReconcilerApp.createThenSetup(() -> w, () -> { throw boom; }));
        assertSame(boom, thrown, "the original setup failure propagates");
        assertEquals(1, w.closes.get(), "the created watch must be closed to avoid a leak");
    }

    @Test
    void setup_failure_with_a_failing_close_suppresses_the_close_error() {
        FakeWatch w = new FakeWatch();
        w.throwOnClose = true;
        RuntimeException boom = new RuntimeException("setup failed");
        RuntimeException thrown = assertThrows(RuntimeException.class,
                () -> ConfigReconcilerApp.createThenSetup(() -> w, () -> { throw boom; }));
        assertSame(boom, thrown);
        assertEquals(1, w.closes.get(), "close was attempted");
        assertEquals(1, thrown.getSuppressed().length, "the close error is suppressed on the original");
        assertInstanceOf(IllegalStateException.class, thrown.getSuppressed()[0]);
    }

    @Test
    void successful_setup_returns_the_watch_unclosed() {
        FakeWatch w = new FakeWatch();
        Watch returned = ConfigReconcilerApp.createThenSetup(() -> w, () -> { /* ok */ });
        assertSame(w, returned);
        assertEquals(0, w.closes.get(), "a watch handed back to the lifecycle must NOT be closed here");
    }
}
