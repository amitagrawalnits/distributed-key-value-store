package com.amit.cs.distributedkeyvaluestore.component;

import com.amit.cs.distributedkeyvaluestore.properties.Seed;
import com.amit.cs.distributedkeyvaluestore.service.ClusterClient;
import com.amit.store.grpc.NodeInfo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.util.List;

@Slf4j
@RequiredArgsConstructor
@Component
public class NodeLifecycleManager {

  @Value("${spring.grpc.server.port}")
  private int myPort;

  private final Seed seed;
  private final ClusterClient client;
  private final ConsistentHashRouter router;
  private final StorageEngine storage;

  /**
   * Handles the {@link ApplicationReadyEvent} to perform initialization routines for the node
   * when the application is fully started. This includes self-registration and potential cluster
   * joining logic.
   * <p>
   * The method performs the following tasks:
   * - Constructs the node's unique identifier using its IP and port.
   * - Registers the current node in the consistent hash router.
   * - If a seed node is specified and valid (i.e., not empty and has a different port),
   * attempts to join the cluster via the seed node by invoking the cluster client's join operation.
   * <p>
   * Upon successful cluster join, any existing nodes in the cluster are added to the router,
   * and a recovery process is initiated to fetch the data range that the current node is responsible for.
   */
  @EventListener(ApplicationReadyEvent.class)
  public void onApplicationReady() {
    final var myId = getMyIp() + ":" + myPort;
    final var me = NodeInfo.newBuilder().setId(myId).setIp(getMyIp()).setPort(myPort).build();
    router.addNode(me);

    if (!seed.ip().isEmpty() && seed.port() != 0 && seed.port() != myPort) { // should join the cluster
      log.info("Joining cluster via Seed: {}:{}", seed.ip(), seed.port());
      client.joinCluster(seed, getMyIp(), myPort).subscribe(res -> {
        if (res.getSuccess()) {
          final var peers = res.getExistingNodesList();
          peers.forEach(router::addNode);
          startRecovery(peers, me);
        }
      });
    }
  }

  /**
   * Initiates the data recovery process for the node by fetching and storing key-value data
   * from peer nodes in the cluster. This ensures that the current node retrieves the data
   * it is responsible for based on the consistent hashing scheme in the cluster.
   *
   * @param peers A list of {@code NodeInfo} representing the nodes in the cluster.
   *              This includes all known nodes except the current node.
   * @param me    The {@code NodeInfo} representation of the current node.
   */
  private void startRecovery(List<NodeInfo> peers, NodeInfo me) {
    for (final var peer : peers) {
      if (peer.getId().equals(me.getId())) continue;
      client.fetchRange(peer, Long.MIN_VALUE, Long.MAX_VALUE).subscribe(kv -> {
        if (router.isNodeResponsibleForKey(me, kv.getKey())) {
          storage.put(kv.getKey(), kv.getValue(), kv.getTimestamp(), kv.getIsTombstone());
        }
      });
    }
  }

  private String getMyIp() {
    try {
      return InetAddress.getLocalHost().getHostAddress();
    } catch (Exception e) {
      return "127.0.0.1"; // Fallback
    }
  }
}