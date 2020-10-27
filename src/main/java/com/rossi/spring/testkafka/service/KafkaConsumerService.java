package com.rossi.spring.testkafka.service;

import com.rossi.spring.testkafka.common.JsonUtils;
import com.rossi.spring.testkafka.model.TestRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import static com.rossi.spring.testkafka.common.GlobalConstant.*;

@Service
@Slf4j
public class KafkaConsumerService {

    @Autowired
    private JsonUtils jsonUtils;

    @KafkaListener(topics = TOPIC, groupId = GROUP_ID, containerFactory = "kafkaListenerContainerFactory")
    public void consume(String message){
        log.info("Consumed message: " + message);
    }

    @KafkaListener(topics = TOPIC + "-dlt", groupId = GROUP_ID, containerFactory = "kafkaListenerContainerFactoryDlt")
    public void consumeDlt(String message){
        log.info("Consumed DLT message: "+ message);
        consume(message);
    }

    @KafkaListener(topics = TOPIC_JSON, groupId = GROUP_ID, containerFactory = "testRequestKafkaListenerContainerFactory")
    public void consumeJson(String request,  Acknowledgment acknowledgment) {
        TestRequest json = jsonUtils.convertJson(request, TestRequest.class);
        log.info("Consumed json message: "+ request);
        log.info("FROM      : {}", json.getFrom());
        log.info("TO        : {}", json.getTo());
        log.info("MESSAGE   : {}", json.getMessage());
        acknowledgment.acknowledge();
    }

    @KafkaListener(topics = TOPIC_JSON +"-dlt", groupId = GROUP_ID, containerFactory = "testRequestKafkaListenerContainerFactoryDlt")
    public void consumeDltJson(String request, Acknowledgment acknowledgment) {
        log.info("Consumed DLT message: "+ request);
        consumeJson(request, acknowledgment);
    }
}
