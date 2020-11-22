package com.github.jishnuc.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Scanner;
import java.util.concurrent.ExecutionException;

public class FavouriteColorProducerApp extends KafkaProducerApp {
    private static Logger logger = LogManager.getLogger(FavouriteColorProducerApp.class);
    public FavouriteColorProducerApp(String topic) throws ExecutionException, InterruptedException {
        super(topic);
    }

    @Override
    public void run() {
        Producer<String,String> producer=new KafkaProducer<String, String>(properties);
        Scanner in= new Scanner(System.in);
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            if(producer!=null)producer.close();
            if(in!=null)in.close();
        }));

        while (true){
            logger.info(">");
            ProducerRecord<String,String> record=new ProducerRecord<>(topic, in.nextLine());
            producer.send(record);
        }
    }
}
