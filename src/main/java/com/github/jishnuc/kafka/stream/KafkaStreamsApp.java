package com.github.jishnuc.kafka.stream;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public abstract class KafkaStreamsApp {

    protected Properties properties;

    public KafkaStreamsApp(String... topics) throws ExecutionException, InterruptedException {
        properties=new Properties();

        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        AdminClient admin = AdminClient.create(properties);
        System.out.println("-- creating  topics--");
        //creating topics if not already there
        admin.createTopics(Arrays.stream(topics).map(topic->new NewTopic(topic, 1, (short)1))
                .collect(Collectors.toList()));
        //listing
        System.out.println("-- listing topics--");
        admin.listTopics().names().get().forEach(System.out::println);

        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-example");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    }

    public abstract void run();


}
