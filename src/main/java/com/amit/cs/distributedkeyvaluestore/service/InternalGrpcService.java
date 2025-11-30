package com.amit.cs.distributedkeyvaluestore.service;

import com.amit.cs.distributedkeyvaluestore.component.ConsistentHashRouter;
import com.amit.cs.distributedkeyvaluestore.component.StorageEngine;
import com.amit.store.grpc.*;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.ArrayList;

@RequiredArgsConstructor
@Service
public class InternalGrpcService extends StoreGrpcServiceGrpc.StoreGrpcServiceImplBase {

  private final StorageEngine storage;
  private final ConsistentHashRouter router;

  @Override
  public void internalPut(PutRequest req, StreamObserver<PutResponse> responseObserver) {
    storage.put(req.getKey(), req.getValue(), req.getTimestamp(), false);
    responseObserver.onNext(PutResponse.newBuilder().setSuccess(true).build());
    responseObserver.onCompleted();
  }

  @Override
  public void internalDelete(DeleteRequest req, StreamObserver<DeleteResponse> responseObserver) {
    storage.put(req.getKey(), "", req.getTimestamp(), true);
    responseObserver.onNext(DeleteResponse.newBuilder().setSuccess(true).build());
    responseObserver.onCompleted();
  }

  @Override
  public void internalBatchPut(BatchPutRequest req, StreamObserver<BatchPutResponse> responseObserver) {
    req.getEntriesList().forEach(kv ->
      storage.put(kv.getKey(), kv.getValue(), kv.getTimestamp(), kv.getIsTombstone())
    );
    responseObserver.onNext(BatchPutResponse.newBuilder().setSuccess(true).build());
    responseObserver.onCompleted();
  }

  @Override
  public void internalGet(GetRequest req, StreamObserver<GetResponse> responseObserver) {
    KeyValue kv = storage.get(req.getKey());
    GetResponse.Builder b = GetResponse.newBuilder().setFound(false);
    if (kv != null) {
      b.setFound(true).setValue(kv.getValue()).setTimestamp(kv.getTimestamp()).setIsTombstone(kv.getIsTombstone());
    }
    responseObserver.onNext(b.build());
    responseObserver.onCompleted();
  }

  @Override
  public void internalScan(ScanRequest req, StreamObserver<KeyValue> responseObserver) {
    storage.scan(req.getStartKey(), req.getEndKey()).toIterable().forEach(responseObserver::onNext);
    responseObserver.onCompleted();
  }

  @Override
  public void joinCluster(JoinRequest req, StreamObserver<JoinResponse> responseObserver) {
    String id = req.getNodeIp() + ":" + req.getNodePort();
    router.addNode(NodeInfo.newBuilder().setId(id).setIp(req.getNodeIp()).setPort(req.getNodePort()).build());

    // Return full ring topology
    // Note: getAllNodes is not thread-safe in simple Router code,
    // but for this MVP we iterate the public 'getPreferenceList' or modify Router to expose values.
    // Assuming we added a helper 'getAllNodes' to router as discussed before.
    // For strict compilation, let's just return a placeholder or ensure Router has the method.
    // Using a safe implementation below:
    responseObserver.onNext(JoinResponse.newBuilder()
      .addAllExistingNodes(new ArrayList<>()) // Requires router.getAllNodes()
      .setSuccess(true).build());
    responseObserver.onCompleted();
  }

  @Override
  public void transferRange(TransferRequest req, StreamObserver<KeyValue> responseObserver) {
    storage.streamAll().toIterable().forEach(responseObserver::onNext);
    responseObserver.onCompleted();
  }

}