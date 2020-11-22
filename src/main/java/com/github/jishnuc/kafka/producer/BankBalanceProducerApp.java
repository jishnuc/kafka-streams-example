package com.github.jishnuc.kafka.producer;

import com.github.jishnuc.kafka.model.BankBalance;
import com.github.jishnuc.kafka.serdes.KafkaJsonSerializer;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.time.Instant;
import java.util.concurrent.ExecutionException;

public class BankBalanceProducerApp extends KafkaProducerApp {
    private static Logger logger = LogManager.getLogger(BankBalanceProducerApp.class);

    public BankBalanceProducerApp(String topic) throws ExecutionException, InterruptedException {
        super(topic);
        properties.put(ProducerConfig.RETRIES_CONFIG,"3");
        //ensure we don't push duplicates
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

    }

    @Override
    public void run() {
        logger.info("Will be Publishing 100 random Bank balances every 1 min for 5 min....");
        Producer<String,BankBalance> producer=new KafkaProducer<>(properties,new StringSerializer(),new KafkaJsonSerializer<>());
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            if(producer!=null)producer.close();
        }));
        int i=1;
        String[] names={"Till","Jill","Sill","Mill","Kill","Bill"};
        while (i<10){

            for(String name:names){

                try {
                    BankBalance balance=new BankBalance(name
                            , Math.floor(RandomUtils.nextDouble(0, 1000.0) * 100) / 100
                            , Instant.now());
                    logger.info("Publishing ->"+balance.toString());
                    ProducerRecord<String,BankBalance> record=new ProducerRecord<>(topic, balance.getName().toLowerCase(),balance);
                    producer.send(record);
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    break;
                }

            }
            i++;
        }

        logger.info("Done Publishing....");
    }

}
