package com.github.jishnuc.kafka.stream;

import com.github.jishnuc.kafka.model.BankBalance;
import com.github.jishnuc.kafka.serdes.KafkaJsonDeserializer;
import com.github.jishnuc.kafka.serdes.KafkaJsonSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class BankBalanceStreamsApp extends KafkaStreamsApp {

    private final Serializer<BankBalance> bankBalanceSerializer;
    private final Deserializer<BankBalance> bankBalanceDeserializer;
    private final Serde<BankBalance> bankBalanceSerde;
    public BankBalanceStreamsApp(String... topics) throws ExecutionException, InterruptedException {
        super(topics);

        bankBalanceSerializer=new KafkaJsonSerializer<>();
        bankBalanceDeserializer=new KafkaJsonDeserializer<>();
        //Configure Deserializer
        Map<String, Object> serdeProps = new HashMap<>();
        serdeProps.put("JsonPojoClass",BankBalance.class);
        bankBalanceDeserializer.configure(serdeProps,false);

        bankBalanceSerde=Serdes.serdeFrom(bankBalanceSerializer,bankBalanceDeserializer);

        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        properties.put(StreamsConfig.producerPrefix(ProducerConfig.MAX_BLOCK_MS_CONFIG), 600000);
    }

    private BankBalance recalculate(BankBalance transactionBalance, BankBalance aggBalance) {
        BankBalance balance = new BankBalance();

        balance.setCount(aggBalance.getCount() + 1);
        balance.setName(transactionBalance.getName());
        balance.setBalance(aggBalance.getBalance() + transactionBalance.getTransaction());
        Instant transactionInstant=transactionBalance.getTime();
        Instant aggInstant=aggBalance.getTime();
        if(aggInstant==null && transactionInstant!=null){
            balance.setTime(transactionInstant);
        }else if(aggInstant!=null && transactionInstant==null){
            balance.setTime(aggInstant);
        }else if(aggInstant!=null && transactionInstant!=null){
            balance.setTime(Instant.ofEpochMilli(Math.max(transactionBalance.getTime().toEpochMilli(),
                    aggBalance.getTime().toEpochMilli())));

        }


        return balance;
    }

    @Override
    public void run() {
        KafkaStreams streams =null;
        try {
            StreamsBuilder builder = new StreamsBuilder();
            KStream<String, BankBalance> bankBalanceStream = builder.stream("bank-balance-input", Consumed.with(Serdes.String(),bankBalanceSerde));

            bankBalanceStream.filter((k, v) -> (v.getName() != null)
                    && (v.getTransaction() >= 0.0)
                    && (v.getTime() != null))
                    .groupByKey()
                    .aggregate(() -> new BankBalance(),
                            (aggKey, newBalance, aggBalance) -> recalculate(newBalance, aggBalance)
                            , Materialized.with(Serdes.String(),bankBalanceSerde))
                    .toStream()
                    .peek((k,v)->System.out.println("key: "+k+" val: "+v))
                    .to("bank-balance-output", Produced.valueSerde(bankBalanceSerde));

            Topology topology = builder.build(properties);
             streams = new KafkaStreams(topology, properties);
            streams.cleanUp();
            streams.start();
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if(streams!=null)
            Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        }

    }
}


