package com.amit.cs.distributedkeyvaluestore;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@OpenAPIDefinition
@ConfigurationPropertiesScan
@SpringBootApplication
public class DistributedKeyValueStoreApplication {

  static void main(String[] args) {
    SpringApplication.run(DistributedKeyValueStoreApplication.class, args);
  }

}
