package com.mbronshteyn;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.*;

import java.util.Date;

@Builder
@Getter
@ToString
class Transaction {

  @JsonIgnore
  private static ObjectMapper objectMapper = new ObjectMapper();

  private String name;
  private double amount;
  private String timestamp;

  String toJson() throws Exception{
    return objectMapper.writeValueAsString( this );
  }

  public Transaction() {
  }

  public Transaction(String name, double amount, String timestamp) {
    this.name = name;
    this.amount = amount;
    this.timestamp = timestamp;
  }
}
