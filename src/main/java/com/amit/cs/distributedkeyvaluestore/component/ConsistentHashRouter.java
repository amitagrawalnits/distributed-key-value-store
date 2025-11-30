package com.amit.cs.distributedkeyvaluestore.component;

import com.amit.store.grpc.NodeInfo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Slf4j
@Component
public class ConsistentHashRouter {

  private final TreeMap<Long, NodeInfo> ring = new TreeMap<>();
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  public void addNode(NodeInfo node) {
    lock.writeLock().lock();
    try {
      long hash = hash(node.getId());
      ring.put(hash, node);
      log.info("Added node {} at hash {}", node.getId(), hash);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void removeNode(NodeInfo node) {
    lock.writeLock().lock();
    try {
      long hash = hash(node.getId());
      ring.remove(hash);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Returns the Primary Owner + Successors (Replicas).
   * e.g., If Key hashes to Node A, returns [Node A, Node B, Node C]
   */
  public List<NodeInfo> getPreferenceList(String key) {
    lock.readLock().lock();
    try {
      List<NodeInfo> nodes = new ArrayList<>();
      if (ring.isEmpty()) return nodes;

      // 1. Find the Primary Owner
      long hash = hash(key);
      Map.Entry<Long, NodeInfo> entry = ring.ceilingEntry(hash);

      if (entry == null) {
        // Wrap around to the start of the ring (Circle logic)
        entry = ring.firstEntry();
      }

      // Add Primary
      nodes.add(entry.getValue());

      // 2. Find Successors (Replicas)
      // We just walk clockwise to find the next N distinct nodes
      Map.Entry<Long, NodeInfo> cursor = entry;

      // We want 3 nodes total (1 Primary + 2 Replicas)
      // But we must stop if the ring is smaller than 3
      int targetSize = Math.min(3, ring.size());

      while (nodes.size() < targetSize) {
        cursor = ring.higherEntry(cursor.getKey());

        // Wrap around check
        if (cursor == null) {
          cursor = ring.firstEntry();
        }

        NodeInfo nextNode = cursor.getValue();

        // Since we removed virtual nodes, strictly speaking, every entry in the TreeMap
        // is a unique physical node. But keeping this check is safe.
        if (nodes.stream().noneMatch(n -> n.getId().equals(nextNode.getId()))) {
          nodes.add(nextNode);
        }
      }

      return nodes;
    } finally {
      lock.readLock().unlock();
    }
  }

  // Helper: Return all nodes (used for Join Response)
  public List<NodeInfo> getAllNodes() {
    lock.readLock().lock();
    try {
      return new ArrayList<>(ring.values());
    } finally {
      lock.readLock().unlock();
    }
  }

  // MD5 Hash Function
  private long hash(String key) {
    try {
      MessageDigest md = MessageDigest.getInstance("MD5");
      byte[] digest = md.digest(key.getBytes());
      long h = 0;
      for (int i = 0; i < 8; i++) {
        h <<= 8;
        h |= (digest[i] & 0xFF);
      }
      return h;
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("MD5 not found", e);
    }
  }
}