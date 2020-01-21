package com.rossi.spring.testkafka.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rossi.spring.testkafka.common.GlobalConstant;
import com.rossi.spring.testkafka.common.JsonUtils;
import com.rossi.spring.testkafka.model.TestRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import static com.rossi.spring.testkafka.common.GlobalConstant.TOPIC;
import static com.rossi.spring.testkafka.common.GlobalConstant.TOPIC_JSON;

@Controller
@RequestMapping("kafka")
@Slf4j
public class KafkaController {
    @Autowired
    private KafkaTemplate kafkaTemplate;

    @Autowired
    private JsonUtils jsonUtils;

    @PostMapping("/publish")
    public ResponseEntity<String> post(@Validated String message){
        log.info(message);
        kafkaTemplate.send(TOPIC, message);
        return ResponseEntity.ok("Published Successfully");
    }

    @PostMapping("/publishJson")
    public ResponseEntity<TestRequest> post(@Validated TestRequest request){
        String requestJson = jsonUtils.toJsonString(request);
        log.info(requestJson);
        kafkaTemplate.send(TOPIC_JSON, requestJson);
        return ResponseEntity.ok(request);
    }

}
