package com.amit.cs.distributedkeyvaluestore.service;

import com.amit.cs.distributedkeyvaluestore.properties.Seed;
import com.amit.store.grpc.*;
import io.grpc.ManagedChannel;
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

  // Cache channels to avoid re-opening TCP connections
  private final Map<String, StoreGrpcServiceGrpc.StoreGrpcServiceStub> stubs = new ConcurrentHashMap<>();

  // Helper to get or create a stub for a specific IP:Port
  private StoreGrpcServiceGrpc.StoreGrpcServiceStub getStub(String ip, int port) {
    String key = ip + ":" + port;
    return stubs.computeIfAbsent(key, k -> {
      ManagedChannel channel = ManagedChannelBuilder.forAddress(ip, port)
        .usePlaintext()
        .build();
      return StoreGrpcServiceGrpc.newStub(channel);
    });
  }

  // --- REPLICATION METHODS ---

  public Mono<Boolean> replicatePut(NodeInfo target, PutRequest request) {
    return Mono.create(sink -> {
      getStub(target.getIp(), target.getPort()).internalPut(request, new StreamObserver<PutResponse>() {
        @Override
        public void onNext(PutResponse value) {
          sink.success(value.getSuccess());
        }

        @Override
        public void onError(Throwable t) {
          // Log error (Node Down logic)
          sink.success(false);
        }

        @Override
        public void onCompleted() {
        }
      });
    });
  }

  public Mono<Boolean> replicateDelete(NodeInfo target, DeleteRequest request) {
    return Mono.create(sink -> {
      getStub(target.getIp(), target.getPort()).internalDelete(request, new StreamObserver<DeleteResponse>() {
        @Override
        public void onNext(DeleteResponse value) {
          sink.success(value.getSuccess());
        }

        @Override
        public void onError(Throwable t) {
          sink.success(false);
        }

        @Override
        public void onCompleted() {
        }
      });
    });
  }

  // --- NEW: BATCH REPLICATION ---
  public Mono<Boolean> replicateBatch(NodeInfo target, List<KeyValue> entries) {
    return Mono.create(sink -> {
      BatchPutRequest req = BatchPutRequest.newBuilder().addAllEntries(entries).build();
      getStub(target.getIp(), target.getPort()).internalBatchPut(req, new StreamObserver<BatchPutResponse>() {
        @Override
        public void onNext(BatchPutResponse value) {
          sink.success(value.getSuccess());
        }

        @Override
        public void onError(Throwable t) {
          // If node is down, the whole batch fails for this replica
          sink.success(false);
        }

        @Override
        public void onCompleted() {
        }
      });
    });
  }

  // --- READ REPAIR & SCANS ---

  public Mono<GetResponse> getFromReplica(NodeInfo target, GetRequest request) {
    return Mono.create(sink -> {
      getStub(target.getIp(), target.getPort()).internalGet(request, new StreamObserver<GetResponse>() {
        @Override
        public void onNext(GetResponse value) {
          sink.success(value);
        }

        @Override
        public void onError(Throwable t) {
          sink.success(GetResponse.newBuilder().setFound(false).build());
        }

        @Override
        public void onCompleted() {
        }
      });
    });
  }

  // --- NEW: SCAN (SCATTER-GATHER) ---
  public Flux<KeyValue> scan(NodeInfo target, String startKey, String endKey) {
    return Flux.create(sink -> {
      ScanRequest req = ScanRequest.newBuilder().setStartKey(startKey).setEndKey(endKey).build();
      getStub(target.getIp(), target.getPort()).internalScan(req, new StreamObserver<KeyValue>() {
        @Override
        public void onNext(KeyValue value) {
          sink.next(value);
        }

        @Override
        public void onError(Throwable t) {
          // If a node fails during scan, we error out the flux so the service can handle partial results or retry
          sink.error(t);
        }

        @Override
        public void onCompleted() {
          sink.complete();
        }
      });
    });
  }

  // --- CLUSTER OPS ---

  public Mono<JoinResponse> joinCluster(Seed seed, String myIp, int myPort) {
    return Mono.create(sink -> {
      final var joinRequest = JoinRequest.newBuilder().setNodeIp(myIp).setNodePort(myPort).build();
      getStub(seed.ip(), seed.port())
        .joinCluster(joinRequest, new StreamObserver<>() {
          @Override
          public void onNext(JoinResponse value) {
            sink.success(value);
          }

          @Override
          public void onError(Throwable t) {
            sink.error(t);
          }

          @Override
          public void onCompleted() {
          }
        });
    });
  }

  public Flux<KeyValue> fetchRange(NodeInfo target, long startHash, long endHash) {
    return Flux.create(sink -> {
      TransferRequest req = TransferRequest.newBuilder().setStartHash(startHash).setEndHash(endHash).build();
      getStub(target.getIp(), target.getPort()).transferRange(req, new StreamObserver<KeyValue>() {
        @Override
        public void onNext(KeyValue value) {
          sink.next(value);
        }

        @Override
        public void onError(Throwable t) {
          sink.error(t);
        }

        @Override
        public void onCompleted() {
          sink.complete();
        }
      });
    });
  }
}