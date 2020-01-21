package com.rossi.spring.testkafka.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 *
 * @author muhammad
 */
@Slf4j
@Component
public class JsonUtils {
    
    private static final ObjectMapper MAPPER = new ObjectMapper();
    
    public <T> T convertJson(String json, Class<T> type) {
        if (json == null) {
            return null;
        }
        try {
            return MAPPER.readValue(json, type);
        } catch (IOException ex) {
            log.debug("Failed to convert JSON string to object", ex);
        }
        return null;
    } 
    
    public <T> T convertJson(String json, TypeReference<T> type) {
        if (json == null) {
            return null;
        }
        try {
            return MAPPER.readValue(json, type);
        } catch (IOException ex) {
            log.debug("Failed to convert JSON string to object", ex);
        }
        return null;
    }
    
    public String toJsonString(Object object) {
        if (object == null) {
            return null;
        }
        try {
            return MAPPER.writeValueAsString(object);
        } catch (JsonProcessingException ex) {
            log.debug("Failed to convert object to JSON string", ex);
        }
        return null;
    }
    
    public String toPrettyJsonString(Object object) {
        if (object == null) {
            return null;
        }
        try {
            return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(object);
        } catch (JsonProcessingException ex) {
            log.debug("Failed to convert object to JSON string", ex);
        }
        return null;
    }
    
}
