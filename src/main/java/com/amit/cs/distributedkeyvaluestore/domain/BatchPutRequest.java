package com.amit.cs.distributedkeyvaluestore.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;
import lombok.experimental.Accessors;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@Builder
@Accessors(fluent = true)
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class BatchPutRequest {

  @JsonProperty("entries")
  private List<Entry> entries;
}