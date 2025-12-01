package com.amit.cs.distributedkeyvaluestore.service;

import com.amit.cs.distributedkeyvaluestore.component.ConsistentHashRouter;
import com.amit.cs.distributedkeyvaluestore.component.StorageEngine;
import com.amit.cs.distributedkeyvaluestore.domain.Entry;
import com.amit.cs.distributedkeyvaluestore.domain.FailedKey;
import com.amit.cs.distributedkeyvaluestore.domain.GetResponse;
import com.amit.store.grpc.*;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.InetAddress;
import java.util.*;

@RequiredArgsConstructor
@Service
public class StoreService {

  private final StorageEngine storage;
  private final ConsistentHashRouter router;
  private final ClusterClient clusterClient;

  @Value("${spring.grpc.server.port}")
  private int myPort;

  /**
   * Handles the write operation for the provided key and value.
   *
   * @param key   the key associated with the data to be written
   * @param value the value to be written corresponding to the key
   * @return a Mono emitting true if the write operation is successful, false otherwise
   */
  public Mono<Boolean> handleWrite(String key, String value) {
    return doWrite(key, value, false);
  }

  /**
   * Handles the deletion of a specified key.
   *
   * @param key the key to be deleted
   * @return a Mono emitting a Boolean indicating the success or failure of the delete operation
   */
  public Mono<Boolean> handleDelete(String key) {
    return doWrite(key, "", true);
  }

  private Mono<Boolean> doWrite(String key, String value, boolean isTombstone) {
    final var timestamp = System.currentTimeMillis();
    final var replicas = router.getPreferenceList(key);

    final var writeTasks = replicas.stream().map(node -> {
        if (isSelf(node)) {
          storage.put(key, value, timestamp, isTombstone);
          return Mono.just(true);
        } else {
          return isTombstone
            ? clusterClient.replicateDelete(node, DeleteRequest.newBuilder().setKey(key).setTimestamp(timestamp).build())
            : clusterClient.replicatePut(node, PutRequest.newBuilder().setKey(key).setValue(value).setTimestamp(timestamp).build());
        }
      })
      .toList();

    return Flux.merge(writeTasks).filter(s -> s).count().map(acks -> acks >= 2);
  }

  /**
   * Handles a read operation for a given key by querying the appropriate replicas
   * and returning the most up-to-date value based on timestamps.
   *
   * @param key the key for which the value should be retrieved
   * @return a Mono containing the latest {@code GetResponse} object if a value is found;
   * otherwise, an empty Mono
   */
  public Mono<GetResponse> handleRead(String key) {
    final var replicas = router.getPreferenceList(key);
    return Flux.fromIterable(replicas).flatMap(node -> {
        if (isSelf(node)) {
          final var kv = storage.get(key);
          return (kv == null) ? Mono.empty() : Mono.just(toDto(kv));
        } else {
          return clusterClient.getFromReplica(node, GetRequest.newBuilder().setKey(key).build())
            .filter(com.amit.store.grpc.GetResponse::getFound).map(this::toDto);
        }
      })
      .sort((a, b) -> Long.compare(b.timestamp(), a.timestamp()))
      .next();
  }

  /**
   * Handles a batch of entries by grouping them based on the primary node responsible,
   * performing local storage operations or replication to other nodes as needed.
   *
   * @param entries the list of entries to be processed, where each entry contains a key and value
   * @return a Mono that emits a list of failed keys, indicating any keys that failed to be stored or replicated
   */
  public Mono<List<FailedKey>> handleBatch(List<Entry> entries) {
    final var batchGroups = new HashMap<String, List<KeyValue>>();
    final var timestamp = System.currentTimeMillis();

    for (final var entry : entries) {
      final var prefs = router.getPreferenceList(entry.key());
      if (prefs.isEmpty()) continue;
      // Group by the IP:Port of the Primary Node
      final var nodeId = prefs.get(0).getId();
      batchGroups.computeIfAbsent(nodeId, _ -> new ArrayList<>()).add(
        KeyValue.newBuilder().setKey(entry.key()).setValue(entry.value()).setTimestamp(timestamp).build()
      );
    }

    return Flux.fromIterable(batchGroups.entrySet()).flatMap(entry -> {
      final var sampleKey = entry.getValue().get(0).getKey();
      final var target = router.getPreferenceList(sampleKey).get(0);

      if (isSelf(target)) {
        entry.getValue().forEach(kv -> storage.put(kv.getKey(), kv.getValue(), kv.getTimestamp(), false));
        return Mono.just(Collections.<FailedKey>emptyList());
      } else {
        return clusterClient.replicateBatch(target, entry.getValue())
          .map(success -> success ? Collections.<FailedKey>emptyList()
            : entry.getValue().stream().map(k -> new FailedKey(k.getKey(), "Node Unreachable")).toList());
      }
    }).flatMapIterable(l -> l).collectList();
  }

  /**
   * Handles a scan operation across a distributed set of nodes. It retrieves key-value pairs
   * within the specified range and resolves any conflicts by keeping the most recent value for each key.
   *
   * @param start the starting key of the range to scan, inclusive
   * @param end the ending key of the range to scan, inclusive
   * @return a Flux stream of the resolved key-value pairs sorted by key
   */
  public Flux<KeyValue> handleScan(String start, String end) {
    final var allNodes = router.getAllNodes();

    return Flux.fromIterable(allNodes)
      .flatMap(node -> {
        if (isSelf(node)) {
          return storage.scan(start, end);
        } else {
          return clusterClient.scan(node, start, end).onErrorResume(e -> Flux.empty());
        }
      })
      .groupBy(KeyValue::getKey)
      .flatMap(groupedFlux -> groupedFlux
        .reduce((kv1, kv2) -> kv1.getTimestamp() > kv2.getTimestamp() ? kv1 : kv2)
      )
      .sort(Comparator.comparing(KeyValue::getKey));
  }

  private boolean isSelf(NodeInfo node) {
    return node.getIp().equals(getMyIp()) && node.getPort() == myPort;
  }

  private String getMyIp() {
    try {
      return InetAddress.getLocalHost().getHostAddress();
    } catch (Exception e) {
      return "127.0.0.1"; // Fallback
    }
  }

  private GetResponse toDto(KeyValue kv) {
    return new GetResponse(kv.getKey(), kv.getValue(), kv.getTimestamp());
  }

  private GetResponse toDto(com.amit.store.grpc.GetResponse res) {
    return new GetResponse("masked", res.getValue(), res.getTimestamp());
  }
}