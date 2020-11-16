package com.github.jishnuc.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

public class WordCountConsumerApp extends KafkaConsumerApp {
    public WordCountConsumerApp(String topic) throws ExecutionException, InterruptedException {
        super(topic);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "word-count");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
    }

    @Override
    public void run() {
        KafkaConsumer<String,Long> consumer=new KafkaConsumer<>(properties);
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
