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
    } finally {
      lock.writeLock().unlock();
    }
  }

  public List<NodeInfo> getPreferenceList(String key) {
    lock.readLock().lock();
    try {
      List<NodeInfo> nodes = new ArrayList<>();
      if (ring.isEmpty()) return nodes;

      long hash = hash(key);
      Map.Entry<Long, NodeInfo> entry = ring.ceilingEntry(hash);
      if (entry == null) entry = ring.firstEntry();

      nodes.add(entry.getValue());

      Map.Entry<Long, NodeInfo> cursor = entry;
      int targetSize = Math.min(3, ring.size());

      while (nodes.size() < targetSize) {
        cursor = ring.higherEntry(cursor.getKey());
        if (cursor == null) cursor = ring.firstEntry();

        NodeInfo nextNode = cursor.getValue();
        if (nodes.stream().noneMatch(n -> n.getId().equals(nextNode.getId()))) {
          nodes.add(nextNode);
        }
      }
      return nodes;
    } finally {
      lock.readLock().unlock();
    }
  }

  public boolean isNodeResponsibleForKey(NodeInfo myNode, String key) {
    List<NodeInfo> responsibleNodes = getPreferenceList(key);
    for (NodeInfo n : responsibleNodes) {
      if (n.getId().equals(myNode.getId())) return true;
    }
    return false;
  }

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
      throw new RuntimeException(e);
    }
  }
}