package com.github.jishnuc.kafka.consumer;

import org.apache.kafka.clients.consumer.*;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

public class UserPurchaseConsumer extends KafkaConsumerApp{

    public UserPurchaseConsumer(String... topics) throws ExecutionException, InterruptedException {
        super(topics);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "user-purchase-consumer");
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
