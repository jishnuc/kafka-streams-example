package com.github.jishnuc.kafka.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class KafkaJsonSerializer<T> implements Serializer<T> {

    private static  Logger logger = LogManager.getLogger(KafkaJsonSerializer.class);

    @Override
    public byte[] serialize(String s, T t) {
        byte[] retVal = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            retVal = objectMapper.writeValueAsBytes(t);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        return retVal;
    }


}
