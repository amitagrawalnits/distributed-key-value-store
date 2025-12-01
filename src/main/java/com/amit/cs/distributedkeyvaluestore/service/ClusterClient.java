package com.amit.cs.distributedkeyvaluestore.service;

import com.amit.cs.distributedkeyvaluestore.properties.Seed;
import com.amit.store.grpc.*;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class ClusterClient {

  private final Map<String, StoreGrpcServiceGrpc.StoreGrpcServiceStub> stubs = new ConcurrentHashMap<>();

  private StoreGrpcServiceGrpc.StoreGrpcServiceStub getStub(String ip, int port) {
    final var key = ip + ":" + port;
    return stubs.computeIfAbsent(key, _ -> {
      final var channel = ManagedChannelBuilder.forAddress(ip, port).usePlaintext().build();
      return StoreGrpcServiceGrpc.newStub(channel);
    });
  }

  /**
   * Sends a replication request to add or update a key-value pair on a target node in the cluster.
   *
   * @param target The target node to which the replication request will be sent. Contains the IP and port of the node.
   * @param request The request containing the key-value pair to be put in the target node.
   * @return A {@link Mono} that emits a {@code Boolean} indicating whether the operation was successful or not.
   */
  public Mono<Boolean> replicatePut(NodeInfo target, PutRequest request) {
    return Mono.create(sink -> getStub(target.getIp(), target.getPort()).internalPut(request, new StreamObserver<>() {
      public void onNext(PutResponse v) {
        sink.success(v.getSuccess());
      }

      public void onError(Throwable t) {
        sink.success(false);
      }

      public void onCompleted() {
      }
    }));
  }

  /**
   * Sends a replication request to delete a key-value pair on a target node in the cluster.
   *
   * @param target The target node to which the delete request will be sent. Contains the IP and port of the node.
   * @param request The request containing the key to be deleted in the target node.
   * @return A {@link Mono} that emits a {@code Boolean} indicating whether the operation was successful or not.
   */
  public Mono<Boolean> replicateDelete(NodeInfo target, DeleteRequest request) {
    return Mono.create(sink -> getStub(target.getIp(), target.getPort()).internalDelete(request, new StreamObserver<>() {
      public void onNext(DeleteResponse v) {
        sink.success(v.getSuccess());
      }

      public void onError(Throwable t) {
        sink.success(false);
      }

      public void onCompleted() {
      }
    }));
  }

  /**
   * Sends a replication request to add or update multiple key-value pairs on a target node in the cluster.
   *
   * @param target The target node to which the replication request will be sent. Contains the IP and port of the node.
   * @param entries A list of key-value pairs to be replicated on the target node.
   * @return A {@link Mono} that emits a {@code Boolean} indicating whether the operation was successful or not.
   */
  public Mono<Boolean> replicateBatch(NodeInfo target, List<KeyValue> entries) {
    return Mono.create(sink -> {
      final var req = BatchPutRequest.newBuilder().addAllEntries(entries).build();
      getStub(target.getIp(), target.getPort()).internalBatchPut(req, new StreamObserver<>() {
        public void onNext(BatchPutResponse v) {
          sink.success(v.getSuccess());
        }

        public void onError(Throwable t) {
          sink.success(false);
        }

        public void onCompleted() {
        }
      });
    });
  }

  /**
   * Sends a request to retrieve a value from a specific replica node in the cluster based on the key in the request.
   *
   * @param target The target node from which the value will be retrieved. Contains the IP and port of the replica node.
   * @param request The request containing the key whose value is to be fetched from the target node.
   * @return A {@link Mono} that emits a {@link GetResponse} containing the result of the retrieval operation.
   *         If the operation is unsuccessful or an error occurs, it emits a response with {@code found} set to false.
   */
  public Mono<GetResponse> getFromReplica(NodeInfo target, GetRequest request) {
    return Mono.create(sink -> getStub(target.getIp(), target.getPort()).internalGet(request, new StreamObserver<>() {
      public void onNext(GetResponse v) {
        sink.success(v);
      }

      public void onError(Throwable t) {
        sink.success(GetResponse.newBuilder().setFound(false).build());
      }

      public void onCompleted() {
      }
    }));
  }

  /**
   * Scans the key-value pairs from a specific node within a given range of keys.
   *
   * @param target The target node from which the key-value pairs will be retrieved. Contains the IP and port of the node.
   * @param start The starting key of the range to scan.
   * @param end The ending key of the range to scan.
   * @return A {@link Flux} emitting {@link KeyValue} objects containing the key-value pairs from the specified range.
   */
  public Flux<KeyValue> scan(NodeInfo target, String start, String end) {
    return Flux.create(sink -> {
      final var req = ScanRequest.newBuilder().setStartKey(start).setEndKey(end).build();
      getStub(target.getIp(), target.getPort()).internalScan(req, new StreamObserver<>() {
        public void onNext(KeyValue v) {
          sink.next(v);
        }

        public void onError(Throwable t) {
          sink.error(t);
        }

        public void onCompleted() {
          sink.complete();
        }
      });
    });
  }

  /**
   * Initiates a join request to connect the current node to an existing cluster.
   *
   * @param seed The seed node information containing the IP and port of the target node to which the cluster join request is sent.
   * @param myIp The IP address of the current node that wants to join the cluster.
   * @param myPort The port of the current node that wants to join the cluster.
   * @return A {@link Mono} that emits a {@link JoinResponse} representing the response from the target node. If the operation fails, the Mono emits an error.
   */
  public Mono<JoinResponse> joinCluster(Seed seed, String myIp, int myPort) {
    return Mono.create(sink -> {
      final var req = JoinRequest.newBuilder().setNodeIp(myIp).setNodePort(myPort).build();
      getStub(seed.ip(), seed.port()).joinCluster(req, new StreamObserver<>() {
        public void onNext(JoinResponse v) {
          sink.success(v);
        }

        public void onError(Throwable t) {
          sink.error(t);
        }

        public void onCompleted() {
        }
      });
    });
  }

  /**
   * Fetches a range of key-value pairs from the specified target node based on the hash range provided.
   *
   * @param target The target node from which the key-value pairs will be retrieved. Contains the IP and port of the node.
   * @param start The starting hash value of the range to fetch.
   * @param end The ending hash value of the range to fetch.
   * @return A {@link Flux} emitting {@link KeyValue} objects containing the key-value pairs within the specified range.
   */
  public Flux<KeyValue> fetchRange(NodeInfo target, long start, long end) {
    return Flux.create(sink -> {
      final var req = TransferRequest.newBuilder().setStartHash(start).setEndHash(end).build();
      getStub(target.getIp(), target.getPort()).transferRange(req, new StreamObserver<>() {
        public void onNext(KeyValue v) {
          sink.next(v);
        }

        public void onError(Throwable t) {
          sink.error(t);
        }

        public void onCompleted() {
          sink.complete();
        }
      });
    });
  }
}