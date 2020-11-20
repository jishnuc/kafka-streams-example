package com.github.jishnuc.kafka.consumer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public abstract class KafkaConsumerApp {
    private static Logger logger = LogManager.getLogger(KafkaConsumerApp.class);
    protected String[] topics;
    protected Properties properties;

    public KafkaConsumerApp(String... topics) throws ExecutionException, InterruptedException { properties=new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        try(AdminClient admin = AdminClient.create(properties)){
            Set<String> names = admin.listTopics(new ListTopicsOptions().listInternal(false)).names().get();
            if(!names.containsAll(Arrays.asList(topics))){
                throw  new RuntimeException("Please create topics before running application");
            }
        }catch (Exception e){
            logger.error("Unable connect to kafka ",e);
        }
        this.topics=topics;
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    }

    public abstract void run();
}
