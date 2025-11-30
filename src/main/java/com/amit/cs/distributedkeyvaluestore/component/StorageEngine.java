package com.amit.cs.distributedkeyvaluestore.component;

import com.amit.store.grpc.KeyValue;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class StorageEngine {

  private final Map<String, KeyValue> storage = new ConcurrentHashMap<>();

  /**
   * The Core "Last-Write-Wins" Logic.
   * We only update if the incoming timestamp is > existing timestamp.
   *
   * @return true if written, false if ignored (because existing data was newer)
   */
  public boolean put(String key, String value, long timestamp, boolean isTombstone) {
    // Optimistic locking loop to handle race conditions safely
    while (true) {
      KeyValue existing = storage.get(key);

      if (existing != null && existing.getTimestamp() > timestamp) {
        // We have newer data already. Ignore this old write.
        return false;
      }

      KeyValue newValue = KeyValue.newBuilder()
        .setKey(key)
        .setValue(value) // If isTombstone, this might be empty, which is fine
        .setTimestamp(timestamp)
        .setIsTombstone(isTombstone)
        .build();

      // Atomic Compare-and-Swap
      if (existing == null) {
        if (storage.putIfAbsent(key, newValue) == null) return true;
      } else {
        if (storage.replace(key, existing, newValue)) return true;
      }
      // If we failed (another thread updated it), loop again and re-check timestamps.
    }
  }

  public KeyValue get(String key) {
    KeyValue kv = storage.get(key);
    // If it exists but is a tombstone, return NULL (effectively not found)
    if (kv != null && kv.getIsTombstone()) {
      return null;
    }
    return kv;
  }

  /**
   * Used for Range Scans.
   * Since HashMap is not sorted, we must iterate.
   * For production, a TreeMap (SkipList) would be better, but this works for v1.
   */
  public Flux<KeyValue> scan(String startKey, String endKey) {
    return Flux.fromIterable(storage.values())
      .filter(kv -> !kv.getIsTombstone()) // Filter out deleted keys
      .filter(kv -> {
        String k = kv.getKey();
        return k.compareTo(startKey) >= 0 && k.compareTo(endKey) <= 0;
      });
  }

  /**
   * Used for Data Transfer (Migration).
   * Returns ALL data (including tombstones) so the new node gets the delete markers too.
   */
  public Flux<KeyValue> streamAll() {
    return Flux.fromIterable(storage.values());
  }
}