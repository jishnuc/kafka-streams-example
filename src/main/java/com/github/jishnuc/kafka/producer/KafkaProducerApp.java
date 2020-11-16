package com.github.jishnuc.kafka.producer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public abstract class KafkaProducerApp {
    private static Logger logger = LogManager.getLogger(KafkaProducerApp.class);
    protected Properties properties;
    protected String topic;
    public KafkaProducerApp(String topic) throws ExecutionException, InterruptedException {
        properties=new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        AdminClient admin = AdminClient.create(properties);
        logger.info("-- creating  topics--");
        //creating topics if not already there
        admin.createTopics(Collections.singleton(new NewTopic(topic, 1, (short)1)));
        //listing
        logger.info("-- listing topics--");
        admin.listTopics().names().get().forEach(System.out::println);

        this.topic=topic;
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    }

    public abstract void run();
}

