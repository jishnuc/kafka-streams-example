package com.github.jishnuc.kafka;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;

import java.util.concurrent.ExecutionException;

public class FavouriteColorStreamsApp extends KafkaStreamsApp{

    public FavouriteColorStreamsApp(String... topics) throws ExecutionException, InterruptedException {
        super(topics);
    }

    public void run(){
        StreamsBuilder builder=new StreamsBuilder();

        KStream<String, String> userColorInput = builder.stream("user-color-input");

        userColorInput.filter((k,v)->v.contains(","))
                        .selectKey((k,v)->v.split(",")[0].toLowerCase())
                        .mapValues(v->v.split(",")[1].toLowerCase())
                        .filter((user,color)-> "red".equals(color)  || "green".equals(color) || "blue".equals(color))
                        .to("user-color");

        KTable<Object, Long> colorCount = builder.table("user-color")
                .groupBy((k,v)->new KeyValue<>(v, ""))
                .count(Named.as("color-count"));


        colorCount.toStream().to("color-count-output");
        Topology colorTopology=builder.build();

        KafkaStreams streams=new KafkaStreams(colorTopology, properties);
        streams.start();
        System.out.println("-------Streams Application-----");
        System.out.println(colorTopology.describe());
        System.out.println("-------Streams Application-----");
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        
        
    }
}
