package com.github.jishnuc.kafka.producer;

import com.github.jishnuc.kafka.model.BankBalance;
import com.github.jishnuc.kafka.serdes.KafkaJsonSerializer;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Date;
import java.util.concurrent.ExecutionException;

public class BankBalanceProducerApp extends KafkaProducerApp {
    private static Logger logger = LogManager.getLogger(BankBalanceProducerApp.class);

    public BankBalanceProducerApp(String topic) throws ExecutionException, InterruptedException {
        super(topic);

    }

    @Override
    public void run() {
        logger.info("Will be Publishing 100 random Bank balances every 1 min for 5 min....");
        KafkaProducer<String,BankBalance> producer=new KafkaProducer<>(properties,new StringSerializer(),new KafkaJsonSerializer<>());
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            if(producer!=null)producer.close();
        }));
        int i=1;
        String[] names={"jack","jill","john","jis"};
        while (i<500){

            BankBalance balance=new BankBalance(names[RandomUtils.nextInt(0, 3)]
                                                , Math.floor(RandomUtils.nextDouble(1000, 100000.0) * 100) / 100
                                                ,new Date());
            logger.info("Publishing ->"+balance.toString());
            ProducerRecord<String,BankBalance> record=new ProducerRecord<>(topic, balance);
            producer.send(record);
            if(i%100==0){
                try {
                    Thread.sleep(60000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            i++;

        }

        logger.info("Done Publishing....");
    }

}
