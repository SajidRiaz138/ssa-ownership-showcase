package io.ssaownership.reconciler;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Single-threaded in-memory {@link ConfigMapStore} fake for unit tests. It honours the policy
 * parts of the contract (missing-CM-throws-on-write, reject-null, preserve-unrelated, absence vs
 * empty) so a test cannot pass against behaviour the real adapter would not reproduce.
 *
 * NOT thread-safe and NOT evidence of concurrent putIfAbsent correctness — the deterministic
 * conditional-update race must be verified against a real API server (KubeConfigMapStoreIT), not
 * here. This fake only honours the single-threaded policy parts of the contract.
 */
final class InMemoryStore implements ConfigMapStore
{

    private final Map<String, Map<String, String>> cms = new HashMap<>();
    int upsertCalls = 0;
    int putIfAbsentCalls = 0;

    /** When >0, the next N store calls of the matching op throw StoreException (retry tests). */
    int failNextGet = 0;
    int failNextUpsert = 0;
    int failNextPutIfAbsent = 0;

    /** Create an (empty) ConfigMap object, as the chart would. Writes require this to exist. */
    void createConfigMap(String cm)
    {
        cms.computeIfAbsent(cm, k -> new HashMap<>());
    }

    void seedKey(String cm, String key, String value)
    {
        Objects.requireNonNull(value, "value"); // reject null: a present key cannot have null value
        cms.computeIfAbsent(cm, k -> new HashMap<>()).put(key, value);
    }

    /** Delete a key but keep the ConfigMap object (models an external deletion of just the key). */
    void deleteKey(String cm, String key)
    {
        Map<String, String> data = cms.get(cm);
        if (data != null)
        {
            data.remove(key);
        }
    }

    @Override
    public Optional<String> get(String cm, String key)
    {
        if (failNextGet > 0)
        {
            failNextGet--;
            throw new StoreException("injected get failure");
        }
        Map<String, String> data = cms.get(cm);
        if (data == null)
        {
            return Optional.empty(); // missing ConfigMap -> empty (per contract)
        }
        return Optional.ofNullable(data.get(key)); // empty ONLY if key absent; "" is present
    }

    @Override
    public void upsert(String cm, String key, String value)
    {
        Objects.requireNonNull(value, "value"); // reject null (per contract)
        if (failNextUpsert > 0)
        {
            failNextUpsert--;
            throw new StoreException("injected upsert failure");
        }
        upsertCalls++;
        Map<String, String> data = cms.get(cm);
        if (data == null)
        {
            throw new StoreException("ConfigMap " + cm + " does not exist"); // do not create
        }
        data.put(key, value); // preserves unrelated keys
    }

    @Override
    public boolean putIfAbsent(String cm, String key, String value)
    {
        Objects.requireNonNull(value, "value");
        if (failNextPutIfAbsent > 0)
        {
            failNextPutIfAbsent--;
            throw new StoreException("injected putIfAbsent failure");
        }
        putIfAbsentCalls++;
        Map<String, String> data = cms.get(cm);
        if (data == null)
        {
            throw new StoreException("ConfigMap " + cm + " does not exist");
        }
        if (data.containsKey(key))
        {
            return false; // preserve existing
        }
        data.put(key, value);
        return true;
    }
}
