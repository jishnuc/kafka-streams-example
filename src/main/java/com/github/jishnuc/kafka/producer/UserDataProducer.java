package com.github.jishnuc.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.concurrent.ExecutionException;

public class UserDataProducer extends KafkaProducerApp{

    public UserDataProducer() throws ExecutionException, InterruptedException {
        super();
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, "3");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    }

    @Override
    public void run() throws ExecutionException, InterruptedException {
        Producer<String,String> producer=new KafkaProducer<String, String>(properties);

        //1 Send user data and then purchase data for same user
        System.out.println("Example 1 - new user");
        producer.send(userRecord("Jishnu","First=Jishnu,Last=Chatterjee,Email=j.c@gmail.com")).get();
        producer.send(purchaseRecord("Jishnu","Apples and Bananas 1")).get();
        Thread.sleep(10000);

        //2 Send only purchase data with no user data
        System.out.println("Example 2 - non existing user");
        producer.send(purchaseRecord("Bob","Kites and whistles 2")).get();

        Thread.sleep(10000);

        //3 Update user Jishnu along with new transaction
        System.out.println("Example 3 - update User");
        producer.send(userRecord("Jishnu","First=Jishn,Last=Chatt,Email=j.c@gmail.com")).get();
        producer.send(purchaseRecord("Jishnu","Oranges 1")).get();

        Thread.sleep(10000);

        //4 Send user purchase before sending user record
        System.out.println("Example 4 - send user purchase before sending user records, send user purchase after user record");
        producer.send(purchaseRecord("mike","Computers 1")).get();
        producer.send(userRecord("mike","First=Mike,Last=Cole,Email=m.c@gmail.com")).get();
        producer.send(purchaseRecord("mike","Books 2")).get();
        producer.send(userRecord("mike",null)).get(); //delete for cleanup

        Thread.sleep(10000);
        //5 create a user but it gets deleted before user purchase record comes through
        System.out.println("Example 5 - user then delete then data");
        producer.send(userRecord("alice","First=Alice,Last=Grace,Email=a.g@gmail.com")).get();
        producer.send(userRecord("alice",null)).get();
        producer.send(purchaseRecord("alice","Face Mask 5")).get();

        Thread.sleep(10000);

        System.out.println("The End");
        producer.close();





    }

    private ProducerRecord<String, String> userRecord(String key, String value) {
        return new ProducerRecord<>("user-table", key,value);
    }

    private ProducerRecord<String, String> purchaseRecord(String key, String value) {
        return new ProducerRecord<>("user-purchases", key,value );
    }
}
