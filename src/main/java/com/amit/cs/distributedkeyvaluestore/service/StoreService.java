package com.amit.cs.distributedkeyvaluestore.service;

import com.amit.cs.distributedkeyvaluestore.component.ConsistentHashRouter;
import com.amit.cs.distributedkeyvaluestore.component.StorageEngine;
import com.amit.cs.distributedkeyvaluestore.domain.EntryDto;
import com.amit.cs.distributedkeyvaluestore.domain.FailedKeyDto;
import com.amit.cs.distributedkeyvaluestore.domain.GetResponseDto;
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

  // Inject properties to identify "Self"
  @Value("${spring.grpc.server.port}") private int myPort;
  @Value("${server.address:127.0.0.1}") private String myIp;

  // --- WRITE PATH (Quorum = 2) ---
  public Mono<Boolean> handleWrite(String key, String value) {
    return doWrite(key, value, false);
  }

  public Mono<Boolean> handleDelete(String key) {
    return doWrite(key, "", true);
  }

  private Mono<Boolean> doWrite(String key, String value, boolean isTombstone) {
    long timestamp = System.currentTimeMillis();
    List<NodeInfo> replicas = router.getPreferenceList(key);

    List<Mono<Boolean>> writeTasks = replicas.stream()
      .map(node -> {
        if (isSelf(node)) {
          storage.put(key, value, timestamp, isTombstone);
          return Mono.just(true);
        } else {
          if (isTombstone) {
            return clusterClient.replicateDelete(node, DeleteRequest.newBuilder().setKey(key).setTimestamp(timestamp).build());
          } else {
            return clusterClient.replicatePut(node, PutRequest.newBuilder().setKey(key).setValue(value).setTimestamp(timestamp).build());
          }
        }
      }).toList();

    return Flux.merge(writeTasks)
      .filter(success -> success)
      .count()
      .map(acks -> acks >= 2);
  }

  // --- READ PATH (Read Repair + LWW) ---
  public Mono<GetResponseDto> handleRead(String key) {
    List<NodeInfo> replicas = router.getPreferenceList(key);

    return Flux.fromIterable(replicas)
      .flatMap(node -> {
        if (isSelf(node)) {
          KeyValue kv = storage.get(key);
          if (kv == null) return Mono.empty();
          return Mono.just(toDto(kv));
        } else {
          return clusterClient.getFromReplica(node, GetRequest.newBuilder().setKey(key).build())
            .filter(GetResponse::getFound)
            .map(this::toDto);
        }
      })
      // LWW: Sort Descending by Timestamp
      .sort((a, b) -> Long.compare(b.timestamp(), a.timestamp()))
      .next();
  }

  // --- NEW: BATCH WRITE ---
  public Mono<List<FailedKeyDto>> handleBatch(List<EntryDto> entries) {
    // 1. Group keys by their Primary Owner (NodeInfo)
    Map<NodeInfo, List<KeyValue>> batchGroups = new HashMap<>();
    long timestamp = System.currentTimeMillis();

    for (EntryDto entry : entries) {
      // Logic: We define the "Primary" as the first node in the preference list
      List<NodeInfo> prefs = router.getPreferenceList(entry.key());
      if (prefs.isEmpty()) continue;
      NodeInfo primary = prefs.get(0);

      KeyValue kv = KeyValue.newBuilder()
        .setKey(entry.key())
        .setValue(entry.value())
        .setTimestamp(timestamp)
        .build();

      batchGroups.computeIfAbsent(primary, k -> new ArrayList<>()).add(kv);
    }

    // 2. Send sub-batches to Primary Owners in parallel
    return Flux.fromIterable(batchGroups.entrySet())
      .flatMap(entry -> {
        NodeInfo targetNode = entry.getKey();
        List<KeyValue> subBatch = entry.getValue();

        // If I am the primary, write locally (Optimization)
        if (isSelf(targetNode)) {
          subBatch.forEach(kv -> storage.put(kv.getKey(), kv.getValue(), kv.getTimestamp(), false));
          return Mono.just(Collections.<FailedKeyDto>emptyList());
        }
        // Else send to remote node
        else {
          return clusterClient.replicateBatch(targetNode, subBatch)
            .map(success -> {
              if (success) return Collections.<FailedKeyDto>emptyList();
              // If node failed, mark all keys in this chunk as failed
              return subBatch.stream()
                .map(k -> new FailedKeyDto(k.getKey(), "Node Unreachable"))
                .toList();
            });
        }
      })
      .flatMapIterable(list -> list) // Flatten list of lists
      .collectList();
  }

  // --- NEW: SCAN (Scatter-Gather) ---
  public Flux<KeyValue> handleScan(String start, String end) {
    // 1. We must ask ALL nodes because keys are randomly partitioned
    List<NodeInfo> allNodes = router.getAllNodes();

    return Flux.fromIterable(allNodes)
      .flatMap(node -> {
        if (isSelf(node)) {
          return storage.scan(start, end);
        } else {
          return clusterClient.scan(node, start, end);
        }
      })
      // 2. We will get duplicates (because of Replicas).
      // We must Deduplicate using LWW (Last Write Wins)
      .groupBy(KeyValue::getKey)
      .flatMap(groupedFlux -> groupedFlux
        .reduce((kv1, kv2) -> kv1.getTimestamp() > kv2.getTimestamp() ? kv1 : kv2)
      )
      // 3. Sort final result Lexicographically
      .sort(Comparator.comparing(KeyValue::getKey));
  }

  private boolean isSelf(NodeInfo node) {
    // Robust check: Compare IP and Port
    // Note: In docker/k8s, IPs might differ, using a NodeID is safer,
    // but for this implementation, IP:Port is the ID.
    return node.getIp().equals(myIp) && node.getPort() == myPort;
  }

  private GetResponseDto toDto(KeyValue kv) {
    return new GetResponseDto(kv.getKey(), kv.getValue(), kv.getTimestamp());
  }

  private GetResponseDto toDto(GetResponse res) {
    return new GetResponseDto("key-masked", res.getValue(), res.getTimestamp());
  }
}