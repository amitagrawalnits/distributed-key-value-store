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

  @EventListener(ApplicationReadyEvent.class)
  public void onApplicationReady() {
    String myId = getMyIp() + ":" + myPort;
    NodeInfo me = NodeInfo.newBuilder().setId(myId).setIp(getMyIp()).setPort(myPort).build();
    router.addNode(me);

    if (!seed.ip().isEmpty() && seed.port() != 0 && seed.port() != myPort) {
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

  private void startRecovery(List<NodeInfo> peers, NodeInfo me) {
    for (NodeInfo peer : peers) {
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