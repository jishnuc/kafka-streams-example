package com.github.jishnuc.kafka.producer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public abstract class KafkaProducerApp {
    private static Logger logger = LogManager.getLogger(KafkaProducerApp.class);
    protected Properties properties;
    protected String topic;
    public KafkaProducerApp(String topic) throws ExecutionException, InterruptedException {
        properties=new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        try(AdminClient admin = AdminClient.create(properties)){
            Set<String> names = admin.listTopics(new ListTopicsOptions().listInternal(false)).names().get();
            if(!names.containsAll(Arrays.asList(topic))){
                throw  new RuntimeException("Please create topics before running application");
            }
        }catch (Exception e){
            logger.error("Unable connect to kafka ",e);
        }
        this.topic=topic;
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    }

    public abstract void run();
}

