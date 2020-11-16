package com.github.jishnuc.kafka.consumer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public abstract class KafkaConsumerApp {
    protected String[] topics;
    protected Properties properties;

    public KafkaConsumerApp(String... topics) throws ExecutionException, InterruptedException { properties=new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        AdminClient admin = AdminClient.create(properties);
        System.out.println("-- creating  topics--");
        //creating topics if not already there
        admin.createTopics(Arrays.stream(topics).map(topic->new NewTopic(topic, 1, (short)1))
                .collect(Collectors.toList()));
        //listing
        System.out.println("-- listing topics--");
        admin.listTopics().names().get().forEach(System.out::println);
        this.topics=topics;
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    }

    public abstract void run();
}
