package com.amit.cs.distributedkeyvaluestore.properties;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.experimental.Accessors;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Accessors(fluent = true)
@AllArgsConstructor
@ConfigurationProperties(prefix = "seed")
public class Seed {

  private String ip;
  private Integer port;

}
