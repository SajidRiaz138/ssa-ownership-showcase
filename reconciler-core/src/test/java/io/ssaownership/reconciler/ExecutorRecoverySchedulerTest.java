package io.ssaownership.reconciler;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.concurrent.Delayed;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Direct unit tests for {@link ExecutorRecoveryScheduler}'s duration-conversion behaviour. These
 * assert the EXACT value submitted to the backing executor via a RECORDING executor, rather than
 * measuring elapsed time — so sub-millisecond rounding and overflow handling are observed
 * precisely and deterministically.
 */
class ExecutorRecoverySchedulerTest {

    /** Records the (delay, unit) of the last schedule() call; never actually runs the task. */
    static final class RecordingExecutor extends ScheduledThreadPoolExecutor {
        final AtomicLong lastDelay = new AtomicLong(Long.MIN_VALUE);
        final AtomicReference<TimeUnit> lastUnit = new AtomicReference<>();
        RecordingExecutor() { super(1); }
        @Override public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            lastDelay.set(delay);
            lastUnit.set(unit);
            return new NoopFuture();
        }
    }

    static final class NoopFuture implements ScheduledFuture<Object> {
        @Override public long getDelay(TimeUnit unit) { return 0; }
        @Override public int compareTo(Delayed o) { return 0; }
        @Override public boolean cancel(boolean mayInterruptIfRunning) { return true; }
        @Override public boolean isCancelled() { return false; }
        @Override public boolean isDone() { return false; }
        @Override public Object get() { return null; }
        @Override public Object get(long timeout, TimeUnit unit) { return null; }
    }

    @Test @Timeout(10)
    void positive_sub_millisecond_delay_is_submitted_as_one_millisecond_not_zero() {
        RecordingExecutor rec = new RecordingExecutor();
        try {
            ExecutorRecoveryScheduler s = new ExecutorRecoveryScheduler(rec);
            s.schedule(Duration.ofNanos(1), () -> {});            // positive, sub-millisecond
            assertEquals(TimeUnit.MILLISECONDS, rec.lastUnit.get());
            assertEquals(1L, rec.lastDelay.get(),
                    "a positive sub-ms delay must be submitted as 1ms, never silently 0");
        } finally {
            rec.shutdownNow();
        }
    }

    @Test @Timeout(10)
    void exactly_zero_delay_is_submitted_as_zero() {
        RecordingExecutor rec = new RecordingExecutor();
        try {
            ExecutorRecoveryScheduler s = new ExecutorRecoveryScheduler(rec);
            s.schedule(Duration.ZERO, () -> {});
            assertEquals(TimeUnit.MILLISECONDS, rec.lastUnit.get(), "unit must be milliseconds");
            assertEquals(0L, rec.lastDelay.get(), "zero stays zero (not bumped to 1ms)");
        } finally {
            rec.shutdownNow();
        }
    }

    @Test @Timeout(10)
    void above_one_millisecond_truncates_fractional_millis_not_rounds_up() {
        RecordingExecutor rec = new RecordingExecutor();
        try {
            ExecutorRecoveryScheduler s = new ExecutorRecoveryScheduler(rec);
            s.schedule(Duration.ofNanos(1_900_000), () -> {});    // 1.9 ms
            assertEquals(TimeUnit.MILLISECONDS, rec.lastUnit.get(), "unit must be milliseconds");
            assertEquals(1L, rec.lastDelay.get(), "1.9ms truncates to 1ms (toMillis), not up to 2ms");
        } finally {
            rec.shutdownNow();
        }
    }

    @Test @Timeout(10)
    void conversion_overflow_is_rejected_as_illegal_argument() {
        RecordingExecutor rec = new RecordingExecutor();
        try {
            ExecutorRecoveryScheduler s = new ExecutorRecoveryScheduler(rec);
            // A duration whose toMillis() overflows a long:
            Duration huge = Duration.ofSeconds(Long.MAX_VALUE);
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                    () -> s.schedule(huge, () -> {}));
            assertTrue(ex.getMessage().toLowerCase().contains("too large"),
                    "overflow must be an IllegalArgumentException about representability, was: " + ex.getMessage());
            assertEquals(Long.MIN_VALUE, rec.lastDelay.get(),
                    "an overflowing delay must never reach the executor");
        } finally {
            rec.shutdownNow();
        }
    }
}
