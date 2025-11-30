package com.amit.cs.distributedkeyvaluestore.controller;

import com.amit.cs.distributedkeyvaluestore.domain.*;
import com.amit.cs.distributedkeyvaluestore.service.StoreService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collections;

@RequiredArgsConstructor
@RestController
@RequestMapping("/api/v1/store")
public class StoreController {

  private final StoreService storeService;

  @PostMapping("/keys")
  public Mono<ResponseEntity<String>> put(@RequestBody Entry request) {
    return storeService.handleWrite(request.key(), request.value())
      .map(success -> {
        if (success) return ResponseEntity.ok("Written Successfully");
        else return ResponseEntity.status(503).body("Quorum Failed");
      });
  }

  @GetMapping("/keys/{key}")
  public Mono<ResponseEntity<GetResponse>> get(@PathVariable String key) {
    return storeService.handleRead(key)
      .map(ResponseEntity::ok)
      .defaultIfEmpty(ResponseEntity.notFound().build());
  }

  @DeleteMapping("/keys/{key}")
  public Mono<ResponseEntity<Void>> delete(@PathVariable String key) {
    return storeService.handleDelete(key)
      .map(success -> success ? ResponseEntity.noContent().build() : ResponseEntity.status(503).build());
  }

  @PostMapping("/batch")
  public Mono<ResponseEntity<BatchResponse>> batchPut(@RequestBody BatchPutRequest request) {
    return storeService.handleBatch(request.entries())
      .map(failures -> {
        if (failures.isEmpty()) {
          return ResponseEntity.ok(new BatchResponse("Success", Collections.emptyList()));
        } else {
          // 207 Multi-Status indicates partial success
          return ResponseEntity.status(207).body(new BatchResponse("Partial Success", failures));
        }
      });
  }

  @GetMapping("/range")
  public Flux<Entry> scan(@RequestParam String start, @RequestParam String end) {
    return storeService.handleScan(start, end)
      .map(kv -> new Entry(kv.getKey(), kv.getValue()));
  }
}