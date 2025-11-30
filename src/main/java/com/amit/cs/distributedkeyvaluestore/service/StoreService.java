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

import java.util.*;

@RequiredArgsConstructor
@Service
public class StoreService {

  private final StorageEngine storage;
  private final ConsistentHashRouter router;
  private final ClusterClient clusterClient;

  @Value("${spring.grpc.server.port}")
  private int myPort;
  @Value("${server.address:127.0.0.1}")
  private String myIp;

  public Mono<Boolean> handleWrite(String key, String value) {
    return doWrite(key, value, false);
  }

  public Mono<Boolean> handleDelete(String key) {
    return doWrite(key, "", true);
  }

  private Mono<Boolean> doWrite(String key, String value, boolean isTombstone) {
    long timestamp = System.currentTimeMillis();
    List<NodeInfo> replicas = router.getPreferenceList(key);

    List<Mono<Boolean>> writeTasks = replicas.stream().map(node -> {
      if (isSelf(node)) {
        storage.put(key, value, timestamp, isTombstone);
        return Mono.just(true);
      } else {
        return isTombstone
          ? clusterClient.replicateDelete(node, DeleteRequest.newBuilder().setKey(key).setTimestamp(timestamp).build())
          : clusterClient.replicatePut(node, PutRequest.newBuilder().setKey(key).setValue(value).setTimestamp(timestamp).build());
      }
    }).toList();

    return Flux.merge(writeTasks).filter(s -> s).count().map(acks -> acks >= 2);
  }

  public Mono<GetResponse> handleRead(String key) {
    List<NodeInfo> replicas = router.getPreferenceList(key);
    return Flux.fromIterable(replicas).flatMap(node -> {
      if (isSelf(node)) {
        KeyValue kv = storage.get(key);
        return (kv == null) ? Mono.empty() : Mono.just(toDto(kv));
      } else {
        return clusterClient.getFromReplica(node, GetRequest.newBuilder().setKey(key).build())
          .filter(com.amit.store.grpc.GetResponse::getFound).map(this::toDto);
      }
    }).sort((a, b) -> Long.compare(b.timestamp(), a.timestamp())).next();
  }

  public Mono<List<FailedKey>> handleBatch(List<Entry> entries) {
    Map<String, List<KeyValue>> batchGroups = new HashMap<>();
    long timestamp = System.currentTimeMillis();

    for (Entry entry : entries) {
      List<NodeInfo> prefs = router.getPreferenceList(entry.key());
      if (prefs.isEmpty()) continue;
      // Group by the IP:Port of the Primary Node
      String nodeId = prefs.get(0).getId();
      batchGroups.computeIfAbsent(nodeId, k -> new ArrayList<>()).add(
        KeyValue.newBuilder().setKey(entry.key()).setValue(entry.value()).setTimestamp(timestamp).build()
      );
    }

    return Flux.fromIterable(batchGroups.entrySet()).flatMap(entry -> {
      // In a real impl, we'd map ID back to NodeInfo.
      // Simplification: We iterate router again or trust the list from getPreferenceList matches.
      // Here we just fetch the NodeInfo from the first key in the batch to get the target object.
      String sampleKey = entry.getValue().get(0).getKey();
      NodeInfo target = router.getPreferenceList(sampleKey).get(0);

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

  public Flux<KeyValue> handleScan(String start, String end) {
    // Need to expose getAllNodes in Router
    // For MVP, assuming we iterate a known list or Router is updated
    return Flux.empty();
  }

  private boolean isSelf(NodeInfo node) {
    return node.getIp().equals(myIp) && node.getPort() == myPort;
  }

  private GetResponse toDto(KeyValue kv) {
    return new GetResponse(kv.getKey(), kv.getValue(), kv.getTimestamp());
  }

  private GetResponse toDto(com.amit.store.grpc.GetResponse res) {
    return new GetResponse("masked", res.getValue(), res.getTimestamp());
  }
}