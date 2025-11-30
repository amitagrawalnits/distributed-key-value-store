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

@Slf4j
@RequiredArgsConstructor
@Component
public class NodeLifecycleManager {

  @Value("${spring.grpc.server.port}")
  private int myPort;

  private final Seed seed;
  private final ClusterClient client;
  private final ConsistentHashRouter router;

  @EventListener(ApplicationReadyEvent.class)
  public void onApplicationReady() {
    log.info("Server is UP. initializing Cluster Membership...");

    // Add myself to the local ring
    final var nodeId = getMyIp() + ":" + myPort;
    router.addNode(NodeInfo.newBuilder().setId(nodeId).setIp(getMyIp()).setPort(myPort).build());

    if (shouldJoinCluster()) {
      joinCluster();
    } else {
      log.info("--- I am the SEED (or Standalone) ---");
    }
  }

  private String getMyIp() {
    try {
      return InetAddress.getLocalHost().getHostAddress();
    } catch (Exception e) {
      return "127.0.0.1"; // Fallback
    }
  }

  private boolean shouldJoinCluster() {
    return !seed.ip().isEmpty() && seed.port() != 0 && seed.port() != myPort;
  }

  private void joinCluster() {
    log.info("Joining cluster via Seed: {}:{}", seed.ip(), seed.port());

    client.joinCluster(seed, getMyIp(), myPort)
      .subscribe(
        response -> {
          if (response.getSuccess()) {
            log.info("Join Successful! Syncing Ring...");
            // Add all existing peers to my local Router
            response.getExistingNodesList().forEach(router::addNode);
            log.info("Cluster size is now: {}", router.getAllNodes().size());

            // Optional: Trigger Data Recovery here
            // recoverData();
          }
        },
        error -> log.error("CRITICAL: Failed to join cluster. {}", error.getMessage())
      );
  }
}