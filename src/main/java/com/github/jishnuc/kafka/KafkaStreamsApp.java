package com.github.jishnuc.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;

import java.util.Arrays;
import java.util.Properties;

public class KafkaStreamsApp {
    public static void main(String[] args) {
        System.out.println("Hello World");
        Properties prop=new Properties();
        prop.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-example");
        prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        prop.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        prop.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        //Word Count
        StreamsBuilder builder=new StreamsBuilder();

        KStream<String,String> wordCountInput=builder.stream("word-count-input");

        KTable<String,Long> wordCounts=wordCountInput
                                        .mapValues(textLine->textLine.toLowerCase())
                                        .flatMapValues(lowerCaseTextLine-> Arrays.asList(lowerCaseTextLine.split(" ")))
                                        .selectKey((ignoredKey,word)->word)
                                        .groupByKey()
                                        .count(Named.as("Counts"));
        wordCounts.toStream().to("word-count-output");

        Topology topology=builder.build(prop);

        KafkaStreams streams= new KafkaStreams(topology,prop);
        streams.start();

        System.out.println(streams.toString());

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));


    }
}
