package com.github.jishnuc.kafka.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.time.Instant;

@Data
@NoArgsConstructor
@ToString
public class BankBalance{
    private String name;
    private double transaction;
    private Instant time;
    private double balance;
    private int count;
    public BankBalance(String name,double transaction,Instant time){
        this.name=name;
        this.transaction=transaction;
        this.time=time;
    }
}