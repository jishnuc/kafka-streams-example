package com.github.jishnuc.kafka.stream;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;

import java.util.concurrent.ExecutionException;

public class UserEventsEnricherApp extends KafkaStreamsApp {
    public UserEventsEnricherApp(String... topics) throws ExecutionException, InterruptedException {
        super(topics);

    }

    @Override
    public void run() {

        StreamsBuilder builder=new StreamsBuilder();

        //We got a global table out of kafka. This table will be replicated on each kafka streams app
        //the key of global table is user id
        GlobalKTable<String,String> usersGlobalTable=builder.globalTable("user-table");

        //We get a stream of user purchases, with userid as key
        KStream<String,String> userPurchases=builder.stream("user-purchases");

        KStream<String,String> userPurchaseEnrichedJoin=
                    userPurchases.join(usersGlobalTable
                            ,(key,value)->key
                            ,(userPurchase,userInfo)->"Purchase="+userPurchase+", UserInfo="+userInfo);
        userPurchaseEnrichedJoin.to("user-purchases-enriched-inner-join");

        KStream<String,String> userPurchasesEnrichedLeftJoin=
                    userPurchases.leftJoin(usersGlobalTable,
                            (key,value)->key,
                            (userPurchase,userInfo)->{
                                if(userInfo!=null){
                                    return "Purchase="+userPurchase+", UserInfo="+userInfo;
                                }else{
                                    return "Purchase="+userPurchase+", UserInfo=null";
                                }
                            });

        userPurchasesEnrichedLeftJoin.to("user-purchases-enriched-left-join");

        Topology topology=builder.build();
        KafkaStreams streams=new KafkaStreams(builder.build(), properties);

        streams.cleanUp();
        streams.start();

        System.out.println(topology.describe());

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
