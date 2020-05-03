package com.rossi.spring.testkafka.configuration;

import com.rossi.spring.testkafka.model.TestRequest;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {
    @Value("${spring.kafka.bootstrap-servers}")
    private String server;

    @Value("${spring.kafka.producer.compression-type}")
    private String compressionType;

    private Map<String,Object> props(){
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }

    private Map<String,Object> propsJson(){
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return props;
    }

    @Bean
    protected ProducerFactory<String, String> producerFactory(){
        return new DefaultKafkaProducerFactory<>(props());
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate(){
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    protected ProducerFactory<Object, Object> producerFactoryDlt(){
        return new DefaultKafkaProducerFactory<>(props());
    }

    @Bean
    public KafkaTemplate<Object, Object> kafkaTemplateDlt(){
        return new KafkaTemplate<>(producerFactoryDlt());
    }

    @Bean
    protected ProducerFactory<String, TestRequest> producerFactoryJson(){
        return new DefaultKafkaProducerFactory<>(propsJson());
    }

    @Bean
    public KafkaTemplate<String, TestRequest> kafkaTemplateJson(){
        return new KafkaTemplate<>(producerFactoryJson());
    }

    @Bean
    protected ProducerFactory<Object, Object> producerFactoryJsonDlt(){
        return new DefaultKafkaProducerFactory<>(propsJson());
    }

    @Bean
    public KafkaTemplate<Object, Object> kafkaTemplateJsonDlt(){
        return new KafkaTemplate<>(producerFactoryJsonDlt());
    }
}
