package com.amit.cs.distributedkeyvaluestore.component;

import com.amit.store.grpc.KeyValue;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class StorageEngine {
  private final Map<String, KeyValue> storage = new ConcurrentHashMap<>();

  public void put(String key, String value, long timestamp, boolean isTombstone) {
    while (true) {
      KeyValue existing = storage.get(key);
      if (existing != null && existing.getTimestamp() > timestamp) {
        return; // Existing data is newer
      }
      KeyValue newValue = KeyValue.newBuilder()
        .setKey(key).setValue(value).setTimestamp(timestamp).setIsTombstone(isTombstone).build();

      if (existing == null) {
        if (storage.putIfAbsent(key, newValue) == null) return;
      } else {
        if (storage.replace(key, existing, newValue)) return;
      }
    }
  }

  public KeyValue get(String key) {
    KeyValue kv = storage.get(key);
    return (kv != null && kv.getIsTombstone()) ? null : kv;
  }

  public Flux<KeyValue> scan(String start, String end) {
    return Flux.fromIterable(storage.values())
      .filter(kv -> !kv.getIsTombstone())
      .filter(kv -> kv.getKey().compareTo(start) >= 0 && kv.getKey().compareTo(end) <= 0);
  }

  public Flux<KeyValue> streamAll() {
    return Flux.fromIterable(storage.values());
  }
}