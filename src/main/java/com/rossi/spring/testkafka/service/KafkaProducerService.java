package com.rossi.spring.testkafka.service;

import com.rossi.spring.testkafka.common.JsonUtils;
import com.rossi.spring.testkafka.model.TestRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import static com.rossi.spring.testkafka.common.GlobalConstant.TOPIC;
import static com.rossi.spring.testkafka.common.GlobalConstant.TOPIC_JSON;

@Service
@Slf4j
public class KafkaProducerService {

    @Autowired
    private JsonUtils jsonUtils;

    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;

    @Autowired
    private KafkaTemplate<String,TestRequest> kafkaTemplateJson;

    public void sendString (String message){
        log.info("Send Kafka: {}", message);
        kafkaTemplate.send(TOPIC, message);
    }

    public void sendRequest (TestRequest request){
        String requestJson = jsonUtils.toJsonString(request);
        log.info("Send Kafka Json: {}", requestJson);
        kafkaTemplateJson.send(TOPIC_JSON, request);
    }
}
