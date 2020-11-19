package com.github.jishnuc.kafka.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Map;

public class KafkaJsonDeserializer<T> implements Deserializer<T> {
    private static Logger logger = LogManager.getLogger(KafkaJsonDeserializer.class);
    private Class<T> type;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        type=(Class<T>)configs.get("JsonPojoClass");
    }

    @Override
    public T deserialize(String s, byte[] bytes) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        T obj = null;
        try {
            obj = mapper.readValue(bytes, type);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        return obj;
    }
}
