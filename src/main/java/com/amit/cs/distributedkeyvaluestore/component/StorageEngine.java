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
   * Inserts or updates a key-value pair in the storage with a specific timestamp.
   * If an existing entry with a newer timestamp is present, the operation is aborted.
   * This method ensures atomicity for concurrent writes.
   *
   * @param key The key associated with the value to insert or update in the storage.
   * @param value The value to be associated with the specified key.
   * @param timestamp The timestamp for the entry, used to determine the recency of the data.
   * @param isTombstone A flag indicating if the entry represents a tombstone (deleted state).
   */
  public void put(String key, String value, long timestamp, boolean isTombstone) {
    while (true) {
      final var existing = storage.get(key);
      if (existing != null && existing.getTimestamp() > timestamp) {
        return; // Existing data is newer
      }
      final var newValue = KeyValue.newBuilder()
        .setKey(key)
        .setValue(value)
        .setTimestamp(timestamp)
        .setIsTombstone(isTombstone)
        .build();

      if (existing == null) {
        if (storage.putIfAbsent(key, newValue) == null) return;
      } else {
        if (storage.replace(key, existing, newValue)) return;
      }
    }
  }

  /**
   * Retrieves the value associated with the specified key from the storage.
   * If the entry is marked as a tombstone (deleted), null is returned.
   *
   * @param key The key whose associated value is to be returned.
   * @return The key-value pair associated with the specified key, or null if the key
   *         does not exist or is marked as a tombstone.
   */
  public KeyValue get(String key) {
    final var kv = storage.get(key);
    return (kv != null && kv.getIsTombstone()) ? null : kv;
  }

  /**
   * Scans the storage and retrieves all key-value pairs within the specified range of keys.
   * Entries marked as tombstones (deleted) are excluded from the result.
   *
   * @param start The starting key of the range (inclusive).
   * @param end The ending key of the range (inclusive).
   * @return A Flux stream of key-value pairs within the specified range that are not marked as tombstones.
   */
  public Flux<KeyValue> scan(String start, String end) {
    return Flux.fromIterable(storage.values())
      .filter(kv -> !kv.getIsTombstone())
      .filter(kv -> kv.getKey().compareTo(start) >= 0 && kv.getKey().compareTo(end) <= 0);
  }

  /**
   * Streams all key-value pairs stored in the storage. This includes all entries,
   * regardless of whether they are marked as tombstones (deleted).
   *
   * @return A Flux stream containing all key-value pairs in the storage.
   */
  public Flux<KeyValue> streamAll() {
    return Flux.fromIterable(storage.values());
  }
}