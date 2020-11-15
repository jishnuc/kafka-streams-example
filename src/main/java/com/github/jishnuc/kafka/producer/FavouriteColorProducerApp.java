package com.github.jishnuc.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Scanner;
import java.util.concurrent.ExecutionException;

public class FavouriteColorProducerApp extends KafkaProducerApp {

    public FavouriteColorProducerApp(String topic) throws ExecutionException, InterruptedException {
        super(topic);
    }

    @Override
    public void run() {
        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(properties);
        Scanner in= new Scanner(System.in);
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            if(producer!=null)producer.close();
            if(in!=null)in.close();
        }));

        while (true){
            System.out.println(">");
            ProducerRecord<String,String> record=new ProducerRecord<>(topic, in.nextLine());
            producer.send(record);
        }
    }
}
