package io.ssaownership.reconciler;

import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Enforcement-contract tests for {@link Reconciler}. The decisive ones distinguish a source
 * cache from true observed-destination comparison: destination drift and deletion with the SAME
 * instance must be repaired/handled, which a source-cache implementation would fail.
 */
class ReconcilerTest {

    private static final String CM = "demo-config";
    private static final String KEY = "message";

    private Reconciler upsert(InMemoryStore s, AtomicReference<String> src) {
        return new Reconciler("enforce", CM, KEY, WriteStrategy.UPSERT, s, src::get);
    }

    // ---- UPSERT (enforcement) ----------------------------------------------------------------

    @Test
    void upsert_writes_then_skips_when_destination_in_sync() {
        InMemoryStore s = new InMemoryStore(); s.createConfigMap(CM);
        Reconciler r = upsert(s, new AtomicReference<>("A"));
        assertEquals(Reconciler.Outcome.WROTE, r.reconcile());
        assertEquals(Reconciler.Outcome.SKIPPED_IN_SYNC, r.reconcile());
        assertEquals(1, s.upsertCalls);
    }

    @Test
    void upsert_repairs_destination_drift_with_the_SAME_instance() {
        // THE decisive test: write A, an external actor changes it to B, desired is still A.
        // A source-cache implementation would wrongly SKIP. Enforcement must repair to A.
        InMemoryStore s = new InMemoryStore(); s.createConfigMap(CM);
        Reconciler r = upsert(s, new AtomicReference<>("A"));
        assertEquals(Reconciler.Outcome.WROTE, r.reconcile());
        s.seedKey(CM, KEY, "B");                       // external drift
        assertEquals(Reconciler.Outcome.WROTE, r.reconcile(), "must repair drift, not skip");
        assertEquals("A", s.get(CM, KEY).orElseThrow());
    }

    @Test
    void upsert_repairs_destination_deletion_with_the_SAME_instance() {
        // Same store, same reconciler, unchanged desired input. Write A, externally DELETE the
        // key (keeping the ConfigMap and an unrelated key), reconcile again -> must restore A.
        InMemoryStore s = new InMemoryStore(); s.createConfigMap(CM);
        s.seedKey(CM, "other", "x");                   // unrelated content that must survive
        Reconciler r = upsert(s, new AtomicReference<>("A"));

        assertEquals(Reconciler.Outcome.WROTE, r.reconcile());
        assertEquals("A", s.get(CM, KEY).orElseThrow());

        s.deleteKey(CM, KEY);                          // external deletion of just our key
        assertTrue(s.get(CM, KEY).isEmpty(), "precondition: key is gone");

        assertEquals(Reconciler.Outcome.WROTE, r.reconcile(),
                "same instance, unchanged desired: deletion must be repaired");
        assertEquals("A", s.get(CM, KEY).orElseThrow(), "key restored");
        assertEquals("x", s.get(CM, "other").orElseThrow(), "unrelated key preserved");
    }

    @Test
    void upsert_initially_in_sync_does_not_write() {
        InMemoryStore s = new InMemoryStore(); s.createConfigMap(CM);
        s.seedKey(CM, KEY, "A");                       // destination already correct
        Reconciler r = upsert(s, new AtomicReference<>("A"));
        assertEquals(Reconciler.Outcome.SKIPPED_IN_SYNC, r.reconcile());
        assertEquals(0, s.upsertCalls, "no unnecessary write when already in sync");
    }

    @Test
    void get_failure_is_FAILED_and_retryable() {
        InMemoryStore s = new InMemoryStore(); s.createConfigMap(CM);
        Reconciler r = upsert(s, new AtomicReference<>("A"));
        s.failNextGet = 1;                             // the read throws
        assertEquals(Reconciler.Outcome.FAILED, r.reconcile());
        assertTrue(s.get(CM, KEY).isEmpty(), "nothing written on read failure");
        assertEquals(Reconciler.Outcome.WROTE, r.reconcile(), "next pass still succeeds");
        assertEquals("A", s.get(CM, KEY).orElseThrow());
    }

    @Test
    void upsert_write_failure_is_FAILED_and_retryable() {
        InMemoryStore s = new InMemoryStore(); s.createConfigMap(CM);
        Reconciler r = upsert(s, new AtomicReference<>("A"));
        s.failNextUpsert = 1;                          // the WRITE throws (get succeeds first)
        assertEquals(Reconciler.Outcome.FAILED, r.reconcile());
        assertEquals(Reconciler.Outcome.WROTE, r.reconcile(), "identical desired still succeeds");
        assertEquals("A", s.get(CM, KEY).orElseThrow());
    }

    @Test
    void putIfAbsent_write_failure_is_FAILED_and_retryable() {
        InMemoryStore s = new InMemoryStore(); s.createConfigMap(CM);
        Reconciler r = new Reconciler("seed", CM, KEY, WriteStrategy.PUT_IF_ABSENT, s, () -> "d");
        s.failNextPutIfAbsent = 1;
        assertEquals(Reconciler.Outcome.FAILED, r.reconcile());
        assertEquals(Reconciler.Outcome.WROTE, r.reconcile());
        assertEquals("d", s.get(CM, KEY).orElseThrow());
    }

    @Test
    void empty_string_value_counts_as_present_and_in_sync() {
        InMemoryStore s = new InMemoryStore(); s.createConfigMap(CM);
        s.seedKey(CM, KEY, "");                        // present, empty
        Reconciler r = upsert(s, new AtomicReference<>(""));
        assertEquals(Reconciler.Outcome.SKIPPED_IN_SYNC, r.reconcile(),
                "empty string is a present, matching value — not absence");
        assertEquals(0, s.upsertCalls);
    }

    // ---- PUT_IF_ABSENT (seed) ----------------------------------------------------------------

    @Test
    void putIfAbsent_seeds_when_absent_but_preserves_existing_same_instance() {
        InMemoryStore s = new InMemoryStore(); s.createConfigMap(CM);
        Reconciler r = new Reconciler("seed", CM, KEY, WriteStrategy.PUT_IF_ABSENT, s, () -> "default-7");
        assertEquals(Reconciler.Outcome.WROTE, r.reconcile());
        assertEquals("default-7", s.get(CM, KEY).orElseThrow());

        s.seedKey(CM, KEY, "operator-99");             // operator changes it
        assertEquals(Reconciler.Outcome.SKIPPED_PRESENT, r.reconcile(),
                "same instance must still preserve a now-present value");
        assertEquals("operator-99", s.get(CM, KEY).orElseThrow());
    }

    @Test
    void putIfAbsent_false_does_NOT_read_the_destination() {
        // Proves the diagnostic read is truly gone: a store whose get() would fail the test if
        // called. If the engine read after a false putIfAbsent, this throws AssertionError.
        ConfigMapStore noRead = new ConfigMapStore() {
            @Override public Optional<String> get(String cm, String k) {
                throw new AssertionError("reconcile must NOT read after putIfAbsent==false");
            }
            @Override public void upsert(String cm, String k, String v) {
                throw new AssertionError("unexpected upsert");
            }
            @Override public boolean putIfAbsent(String cm, String k, String v) {
                return false;                          // key already present
            }
        };
        Reconciler r = new Reconciler("seed", CM, KEY, WriteStrategy.PUT_IF_ABSENT, noRead, () -> "x");
        assertEquals(Reconciler.Outcome.SKIPPED_PRESENT, r.reconcile());
    }

    // ---- failures & validation ---------------------------------------------------------------

    @Test
    void null_derivation_is_FAILED_not_thrown() {
        InMemoryStore s = new InMemoryStore(); s.createConfigMap(CM);
        Reconciler r = new Reconciler("x", CM, KEY, WriteStrategy.UPSERT, s, () -> null);
        assertEquals(Reconciler.Outcome.FAILED, r.reconcile());
    }

    @Test
    void missing_configmap_on_write_is_FAILED() {
        InMemoryStore s = new InMemoryStore();         // CM not created
        Reconciler r = upsert(s, new AtomicReference<>("A"));
        assertEquals(Reconciler.Outcome.FAILED, r.reconcile(),
                "writing to a non-existent ConfigMap is a (retryable) failure, not a silent create");
    }

    @Test
    void constructor_rejects_null_dependencies() {
        InMemoryStore s = new InMemoryStore();
        assertThrows(NullPointerException.class,
                () -> new Reconciler(null, CM, KEY, WriteStrategy.UPSERT, s, () -> "a"));
        assertThrows(NullPointerException.class,
                () -> new Reconciler("n", CM, KEY, WriteStrategy.UPSERT, null, () -> "a"));
        assertThrows(NullPointerException.class,
                () -> new Reconciler("n", CM, KEY, null, s, () -> "a"));
    }
}
