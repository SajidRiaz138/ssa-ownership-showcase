package io.ssaownership.reconciler;

import io.fabric8.kubernetes.client.Watch;

/**
 * Creates a watch whose close notification is bound to an {@code onClosed} callback supplied by
 * the lifecycle BEFORE the {@link Watch} handle exists. This is what makes close-before-install
 * correct for real fabric8 watches: the watcher can report closure through {@code onClosed} even
 * if it closes before {@code create} returns a handle. The callback does not need the handle to
 * identify its attempt — the lifecycle gives each attempt its own callback instance.
 *
 * <p>Contrast with a test-only "is this watch closed?" probe, which cannot supply this property
 * for production watches and was deliberately removed.
 */
@FunctionalInterface
public interface WatchFactory {
    /**
     * Create a watch, having wired its terminal-closure notification to {@code onClosed} BEFORE
     * returning the handle. Contract:
     * <ul>
     *   <li>{@code onClosed} fires when the watch terminates for any reason (server close, error,
     *       or an explicit {@link Watch#close()}).</li>
     *   <li>Notification MAY occur before {@code create} returns (close-before-install) — the
     *       lifecycle handles that case via generation invalidation.</li>
     *   <li>Duplicate notifications are TOLERATED by the lifecycle (stale ones are ignored), so a
     *       factory need not guarantee exactly-once delivery.</li>
     * </ul>
     * Whether an explicit {@link Watch#close()} reliably produces {@code onClosed} depends on the
     * fabric8 version; the real adapter must verify or normalise this. This interface alone cannot
     * establish that production behaviour.
     */
    Watch create(Runnable onClosed);
}
