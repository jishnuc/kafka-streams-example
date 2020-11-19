package com.github.jishnuc.kafka.consumer;

import com.github.jishnuc.kafka.model.BankBalance;
import com.github.jishnuc.kafka.serdes.KafkaJsonDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class BankBalanceConsumerApp extends KafkaConsumerApp {
    private static Logger                            logger                      =
        LogManager.getLogger(BankBalanceConsumerApp.class);
    private final KafkaJsonDeserializer<BankBalance> bankBalanceJsonDeserializer = new KafkaJsonDeserializer<>();
    private final StringDeserializer                 stringDeserializer          = new StringDeserializer();

    public BankBalanceConsumerApp(String... topics) throws ExecutionException, InterruptedException {
        super(topics);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "bank-balance");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,KafkaJsonDeserializer.class);
        Map<String, Object> props = new HashMap<>();
        props.put("JsonPojoClass", BankBalance.class);
        bankBalanceJsonDeserializer.configure(props, false);
    }

    @Override
    public void run() {
        KafkaConsumer<String, BankBalance> consumer = new KafkaConsumer<>(properties,
                                                                          new StringDeserializer(),
                                                                            bankBalanceJsonDeserializer);

        consumer.subscribe(Arrays.asList(topics));
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::close));

        while (true) {
            ConsumerRecords<String, BankBalance> consumerRecords = consumer.poll(Duration.ofMillis(500));

            for (String topic : topics) {
                Iterable<ConsumerRecord<String, BankBalance>> record = consumerRecords.records(topic);

                record.forEach(
                    r -> {
                        System.out.println("Topic: " + r.topic() + " Time: " + r.timestamp() + " Key: " + r.key()
                                           + " Value: " + r.value());
                    });
            }
        }
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
