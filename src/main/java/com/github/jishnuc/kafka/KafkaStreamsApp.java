package com.github.jishnuc.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class KafkaStreamsApp {

    protected Properties properties;

    public KafkaStreamsApp(String... topics) throws ExecutionException, InterruptedException {
        properties=new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-example");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        AdminClient admin = AdminClient.create(properties);
        System.out.println("-- creating  topics--");
        //creating topics if not already there
        admin.createTopics(Arrays.stream(topics).map(topic->new NewTopic(topic, 1, (short)1))
                .collect(Collectors.toList()));

        //listing
        System.out.println("-- listing topics--");
        admin.listTopics().names().get().forEach(System.out::println);



    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        System.out.println(".............Starting the KafkaStreamsApp.....");
        System.out.println("Enter program you want to run");
        System.out.println("1. Word Count");
        System.out.println("2. Favourite Color");
        Scanner in= new Scanner(System.in);
        String choice = in.nextLine();
        switch(choice){
            case "1": WordCountStreamsApp wc=new WordCountStreamsApp("word-count-input","word-count-output");
                        wc.run();
                        break;
            case "2": FavouriteColorStreamsApp fc=new FavouriteColorStreamsApp("user-color-input","user-color","color-count-output") ;
                        fc.run();
            default:
                throw new IllegalStateException("Unexpected value: " + choice);
        }
    }
}
