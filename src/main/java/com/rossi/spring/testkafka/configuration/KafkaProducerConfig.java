package com.rossi.spring.testkafka.configuration;

import com.rossi.spring.testkafka.common.GlobalConstant;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

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
        props.put(ProducerConfig.CLIENT_ID_CONFIG, GlobalConstant.GROUP_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 300);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 500);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 300);
        return props;
    }

    @Bean
    protected ProducerFactory<String, Object> producerFactory(){
        return new DefaultKafkaProducerFactory<>(props());
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate(){
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

}
