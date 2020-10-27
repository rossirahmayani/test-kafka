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
    private KafkaTemplate<String,Object> kafkaTemplate;

    public void sendString (String message){
        log.info("Send Kafka: {}", message);
        kafkaTemplate.send(TOPIC, message).addCallback(
                res -> log.info("Messaqe [{}] sent to topic [{}], partition [{}], offset [{}] ", message, TOPIC, res.getRecordMetadata().partition(), res.getRecordMetadata().offset()),
                throwable -> log.error("Error: [{}]", throwable.getMessage()));
    }

    public void sendRequest (TestRequest request){
        String requestJson = jsonUtils.toJsonString(request);
        log.info("Send Kafka Json: {}", requestJson);
        kafkaTemplate.send(TOPIC_JSON, requestJson)
                .addCallback(
                        res -> log.info("Messaqe [{}] sent to topic [{}], partition [{}], offset [{}] ", requestJson, TOPIC_JSON, res.getRecordMetadata().partition(), res.getRecordMetadata().offset()),
                        throwable -> log.error("Error: [{}]", throwable.getMessage()));
    }
}
