package com.rossi.spring.testkafka.service;

import com.rossi.spring.testkafka.common.GlobalConstant;
import com.rossi.spring.testkafka.common.JsonUtils;
import com.rossi.spring.testkafka.model.TestRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import static com.rossi.spring.testkafka.common.GlobalConstant.GROUP_ID;
import static com.rossi.spring.testkafka.common.GlobalConstant.GROUP_JSON;

@Service
@Slf4j
public class KafkaConsumerService {

    @Autowired
    private JsonUtils jsonUtils;

    @KafkaListener(topics = GlobalConstant.TOPIC, groupId = GROUP_ID, containerFactory = "kafkaListenerContainerFactory")
    public void consume(String message){
        log.info("Consumed message: " + message);
    }

    @KafkaListener(topics = GlobalConstant.TOPIC_JSON, groupId = GROUP_JSON, containerFactory = "testRequestKafkaListenerContainerFactory")
    public void consume(TestRequest request) {
        String requestJson = jsonUtils.toJsonString(request);
        log.info("Consumed json message: "+ requestJson);
    }
}
