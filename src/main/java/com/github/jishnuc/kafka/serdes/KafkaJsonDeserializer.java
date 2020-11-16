package com.github.jishnuc.kafka.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class KafkaJsonDeserializer<T> implements Deserializer<T> {
    private static Logger logger = LogManager.getLogger(KafkaJsonDeserializer.class);
    private Class<T> type;
    public KafkaJsonDeserializer(Class<T> type) {
        this.type=type;
    }

    @Override
    public T deserialize(String s, byte[] bytes) {
        ObjectMapper mapper = new ObjectMapper();
        T obj = null;
        try {
            obj = mapper.readValue(bytes, type);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        return obj;
    }
}
