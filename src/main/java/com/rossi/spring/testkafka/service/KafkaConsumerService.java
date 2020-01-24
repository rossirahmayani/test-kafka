package com.rossi.spring.testkafka.service;

import com.rossi.spring.testkafka.common.JsonUtils;
import com.rossi.spring.testkafka.model.TestRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
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

    @KafkaListener(topics = TOPIC_JSON, groupId = GROUP_JSON, containerFactory = "testRequestKafkaListenerContainerFactory")
    public void consumeJson(TestRequest request) {
        String requestJson = jsonUtils.toJsonString(request);
        log.info("Consumed json message: "+ requestJson);
    }

    @KafkaListener(topics = TOPIC_JSON +".DLT", groupId = DLT_JSON, containerFactory = "testRequestKafkaListenerContainerFactoryDlt")
    public void consumeDlt(TestRequest request) {
        String requestJson = jsonUtils.toJsonString(request);
        log.info("Consumed DLT message: "+ requestJson);
        consumeJson(request);
    }
}
