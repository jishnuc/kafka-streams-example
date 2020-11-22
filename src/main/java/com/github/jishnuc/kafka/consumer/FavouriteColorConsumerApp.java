package com.github.jishnuc.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

public class FavouriteColorConsumerApp extends KafkaConsumerApp {
    private static Logger logger = LogManager.getLogger(FavouriteColorConsumerApp.class);
    public FavouriteColorConsumerApp(String... topics) throws ExecutionException, InterruptedException {
        super(topics);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "favourite-color");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
    }

    @Override
    public void run() {
        Consumer<String,Long> consumer=new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topics));
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::close));

        while(true){
            ConsumerRecords<String, Long> consumerRecords = consumer.poll(Duration.ofMillis(500));
            for(String topic:topics){
                Iterable<ConsumerRecord<String, Long>> record = consumerRecords.records(topic);
                record.forEach(r->{
                    System.out.println("Topic: "+r.topic()+" Time: "+r.timestamp()+" Key: "+r.key()+" Value: "+r.value());
                });
            }
        }
    }
}
