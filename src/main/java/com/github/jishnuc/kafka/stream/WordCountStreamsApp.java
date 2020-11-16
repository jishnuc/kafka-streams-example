package com.github.jishnuc.kafka.stream;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;

public class WordCountStreamsApp extends KafkaStreamsApp {
    private static Logger logger = LogManager.getLogger(WordCountStreamsApp.class);
    public WordCountStreamsApp(String... topics) throws ExecutionException, InterruptedException {
        super(topics);
    }
    @Override
    public void run(){
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

        Topology topology=builder.build();

        KafkaStreams streams= new KafkaStreams(topology,properties);
        streams.start();
        logger.info("-------Streams Application-----");
        logger.info(topology.describe());
        logger.info("-------Streams Application-----");
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}
