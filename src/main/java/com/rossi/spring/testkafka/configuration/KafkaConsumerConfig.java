package com.rossi.spring.testkafka.configuration;

import com.rossi.spring.testkafka.exception.KafkaErrorHandler;
import com.rossi.spring.testkafka.model.TestRequest;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer2;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.AlwaysRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.HashMap;
import java.util.Map;

import static com.rossi.spring.testkafka.common.GlobalConstant.GROUP_ID;
import static com.rossi.spring.testkafka.common.GlobalConstant.GROUP_JSON;

@Configuration
@EnableKafka
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String kafkaBootStrapServer;

    @Value("${spring.kafka.limit.error.retry}")
    private Integer maxRetry;

    @Bean //STRING
    protected ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootStrapServer);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(KafkaTemplate kafkaTemplateDlt){
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setErrorHandler(new KafkaErrorHandler(kafkaTemplateDlt));
        return factory;
    }

    //STRING DLT
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactoryDlt(){
        FixedBackOffPolicy fixedBackOffPolicy = new FixedBackOffPolicy();
        fixedBackOffPolicy.setBackOffPeriod(2000L);
        RetryTemplate retryTemplate = new RetryTemplate();
        retryTemplate.setRetryPolicy(new AlwaysRetryPolicy());
        retryTemplate.setBackOffPolicy(fixedBackOffPolicy);
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(3);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        factory.getContainerProperties().setAckOnError(false);
        factory.getContainerProperties().setSyncCommits(true);
        factory.setRetryTemplate(retryTemplate);
        return factory;
    }

    @Bean //JSON REQUEST
    protected ConsumerFactory<String, Object> testRequestConsumerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootStrapServer);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_JSON);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
        config.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);
        return new DefaultKafkaConsumerFactory(config,
                new ErrorHandlingDeserializer2(new StringDeserializer()),
                new ErrorHandlingDeserializer2(new JsonDeserializer<>(TestRequest.class)));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> testRequestKafkaListenerContainerFactory(KafkaTemplate kafkaTemplateJsonDlt){
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(testRequestConsumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setAckOnError(false);
        factory.getContainerProperties().setSyncCommits(true);
        factory.setErrorHandler(new KafkaErrorHandler(kafkaTemplateJsonDlt));
        return factory;
    }

    //JSON DLT
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> testRequestKafkaListenerContainerFactoryDlt(){
        FixedBackOffPolicy fixedBackOffPolicy = new FixedBackOffPolicy();
        fixedBackOffPolicy.setBackOffPeriod(2000L);
        RetryTemplate retryTemplate = new RetryTemplate();
        retryTemplate.setRetryPolicy(new AlwaysRetryPolicy());
        retryTemplate.setBackOffPolicy(fixedBackOffPolicy);
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(testRequestConsumerFactory());
        factory.setConcurrency(1);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setAckOnError(false);
        factory.getContainerProperties().setSyncCommits(true);
        factory.setRetryTemplate(retryTemplate);

        return factory;
    }

}
