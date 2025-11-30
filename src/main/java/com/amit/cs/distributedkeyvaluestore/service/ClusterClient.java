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

  private final Map<String, StoreGrpcServiceGrpc.StoreGrpcServiceStub> stubs = new ConcurrentHashMap<>();

  private StoreGrpcServiceGrpc.StoreGrpcServiceStub getStub(String ip, int port) {
    String key = ip + ":" + port;
    return stubs.computeIfAbsent(key, k -> {
      ManagedChannel channel = ManagedChannelBuilder.forAddress(ip, port).usePlaintext().build();
      return StoreGrpcServiceGrpc.newStub(channel);
    });
  }

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

  public Mono<Boolean> replicateBatch(NodeInfo target, List<KeyValue> entries) {
    return Mono.create(sink -> {
      BatchPutRequest req = BatchPutRequest.newBuilder().addAllEntries(entries).build();
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

  public Flux<KeyValue> scan(NodeInfo target, String start, String end) {
    return Flux.create(sink -> {
      ScanRequest req = ScanRequest.newBuilder().setStartKey(start).setEndKey(end).build();
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

  public Mono<JoinResponse> joinCluster(Seed seed, String myIp, int myPort) {
    return Mono.create(sink -> {
      JoinRequest req = JoinRequest.newBuilder().setNodeIp(myIp).setNodePort(myPort).build();
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

  public Flux<KeyValue> fetchRange(NodeInfo target, long start, long end) {
    return Flux.create(sink -> {
      TransferRequest req = TransferRequest.newBuilder().setStartHash(start).setEndHash(end).build();
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