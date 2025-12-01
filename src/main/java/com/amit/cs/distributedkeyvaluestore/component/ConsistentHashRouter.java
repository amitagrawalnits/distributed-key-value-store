package com.amit.cs.distributedkeyvaluestore.component;

import com.amit.store.grpc.NodeInfo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Slf4j
@Component
public class ConsistentHashRouter {

  private final TreeMap<Long, NodeInfo> ring = new TreeMap<>();
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  /**
   * Adds a new node to the consistent hash ring. The node is hashed based
   * on its identifier and placed into the ring structure.
   *
   * @param node the {@code NodeInfo} object representing the node to be added
   *             to the hash ring
   */
  public void addNode(NodeInfo node) {
    lock.writeLock().lock();
    try {
      final var hash = hash(node.getId());
      ring.put(hash, node);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Retrieves a list of nodes that are preferred for the given key in a consistent hashing ring.
   * The preference list includes the node responsible for the key and additional nodes
   * to meet a target size (up to 3 or the total number of nodes in the ring, whichever is smaller).
   *
   * The returned list ensures no duplicate nodes and preserves the consistent hashing properties.
   *
   * @param key the key for which the preference list is to be generated
   * @return a list of {@code NodeInfo} objects representing the preferred nodes for the given key
   */
  public List<NodeInfo> getPreferenceList(String key) {
    lock.readLock().lock();
    try {
      final var nodes = new ArrayList<NodeInfo>();
      if (ring.isEmpty()) return nodes;

      final var hash = hash(key);
      var entry = ring.ceilingEntry(hash);
      if (entry == null) entry = ring.firstEntry();

      nodes.add(entry.getValue());

      var cursor = entry;
      final var targetSize = Math.min(3, ring.size());

      while (nodes.size() < targetSize) {
        cursor = ring.higherEntry(cursor.getKey());
        if (cursor == null) cursor = ring.firstEntry();

        final var nextNode = cursor.getValue();
        if (nodes.stream().noneMatch(n -> n.getId().equals(nextNode.getId()))) {
          nodes.add(nextNode);
        }
      }
      return nodes;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Determines if the specified node is responsible for the given key in the consistent hash ring.
   * The responsibility is determined based on the node's presence in the preference list
   * for the provided key.
   *
   * @param myNode the {@code NodeInfo} object representing the node to check
   * @param key the key for which to check responsibility
   * @return {@code true} if the specified node is responsible for the key; {@code false} otherwise
   */
  public boolean isNodeResponsibleForKey(NodeInfo myNode, String key) {
    final var responsibleNodes = getPreferenceList(key);
    for (final var node : responsibleNodes) {
      if (node.getId().equals(myNode.getId())) return true;
    }
    return false;
  }

  /**
   * Retrieves a list of all unique nodes currently present in the consistent hash ring.
   * The uniqueness of nodes is determined based on their identifier (e.g., IP:Port).
   * Ensures that the list does not contain duplicate node entries.
   *
   * @return a list of {@code NodeInfo} objects representing all unique nodes in the hash ring
   */
  public List<NodeInfo> getAllNodes() {
    lock.readLock().lock();
    try {
      final var uniqueNodes = new ArrayList<NodeInfo>();
      for (final var node : ring.values()) {
        // Check uniqueness based on ID (IP:Port)
        if (uniqueNodes.stream().noneMatch(n -> n.getId().equals(node.getId()))) {
          uniqueNodes.add(node);
        }
      }
      return uniqueNodes;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Computes a hash value for the given key using the MD5 hashing algorithm.
   * Converts the MD5 digest's first 8 bytes into a 64-bit long value.
   *
   * @param key the input key to be hashed
   * @return the 64-bit hash value derived from the MD5 hash of the input key
   * @throws RuntimeException if the MD5 algorithm is not supported on the platform
   */
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