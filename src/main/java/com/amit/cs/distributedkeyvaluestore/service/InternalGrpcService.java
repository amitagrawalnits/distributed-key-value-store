package com.amit.cs.distributedkeyvaluestore.service;

import com.amit.cs.distributedkeyvaluestore.component.StorageEngine;
import com.amit.cs.distributedkeyvaluestore.component.ConsistentHashRouter;
import com.amit.store.grpc.*;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;

@RequiredArgsConstructor
@Service
public class InternalGrpcService extends StoreGrpcServiceGrpc.StoreGrpcServiceImplBase {

    private final StorageEngine storage;
    private final ConsistentHashRouter router;

    @Value("${spring.grpc.server.port}") private int myPort;
    @Value("${server.address:127.0.0.1}") private String myIp;


    @Override
    public void internalPut(PutRequest req, StreamObserver<PutResponse> responseObserver) {
        storage.put(req.getKey(), req.getValue(), req.getTimestamp(), false);
        responseObserver.onNext(PutResponse.newBuilder().setSuccess(true).build());
        responseObserver.onCompleted();
    }

    @Override
    public void internalDelete(DeleteRequest req, StreamObserver<DeleteResponse> responseObserver) {
        // Write a tombstone
        storage.put(req.getKey(), "", req.getTimestamp(), true);
        responseObserver.onNext(DeleteResponse.newBuilder().setSuccess(true).build());
        responseObserver.onCompleted();
    }

    @Override
    public void internalGet(GetRequest req, StreamObserver<GetResponse> responseObserver) {
        KeyValue kv = storage.get(req.getKey());
        GetResponse.Builder builder = GetResponse.newBuilder();
        
        if (kv != null) {
            builder.setFound(true)
                   .setValue(kv.getValue())
                   .setTimestamp(kv.getTimestamp())
                   .setIsTombstone(kv.getIsTombstone());
        } else {
            builder.setFound(false);
        }
        
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void joinCluster(JoinRequest req, StreamObserver<JoinResponse> responseObserver) {
        // 1. Add new node to my ring
        String id = req.getNodeIp() + ":" + req.getNodePort();
        NodeInfo newNode = NodeInfo.newBuilder().setId(id).setIp(req.getNodeIp()).setPort(req.getNodePort()).build();
        router.addNode(newNode);

        // 2. Return the full list of nodes I know about so the new node can discover the mesh
        List<NodeInfo> allNodes = router.getAllNodes();
        
        responseObserver.onNext(JoinResponse.newBuilder()
                .addAllExistingNodes(allNodes)
                .setSuccess(true)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void transferRange(TransferRequest req, StreamObserver<KeyValue> responseObserver) {
        // Stream ALL data. The receiver filters what they need based on hash logic (simplification)
        storage.streamAll().toIterable().forEach(responseObserver::onNext);
        responseObserver.onCompleted();
    }
}