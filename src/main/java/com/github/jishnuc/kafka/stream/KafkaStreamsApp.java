package com.github.jishnuc.kafka.stream;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public abstract class KafkaStreamsApp {
    private static Logger logger = LogManager.getLogger(KafkaStreamsApp.class);
    protected Properties properties;

    public KafkaStreamsApp(String... topics) throws ExecutionException, InterruptedException {
        properties=new Properties();

        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        try(AdminClient admin = AdminClient.create(properties)){
            Set<String> names = admin.listTopics(new ListTopicsOptions().listInternal(false)).names().get();
            if(!names.containsAll(Arrays.asList(topics))){
                throw  new RuntimeException("Please create topics before running application");
            }
        }catch (Exception e){
            logger.error("Unable connect to kafka ",e);
        }

        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-example");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    }

    public abstract void run();


}
