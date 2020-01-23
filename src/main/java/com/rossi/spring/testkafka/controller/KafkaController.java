package com.rossi.spring.testkafka.controller;

import com.rossi.spring.testkafka.model.TestRequest;
import com.rossi.spring.testkafka.service.KafkaProducerService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

@Controller
@Slf4j
public class KafkaController {

    @Autowired
    private KafkaProducerService kafkaProducerService;

    //REQUEST STRING
    @PostMapping("/publish")
    public ResponseEntity<String> post(@Validated String message){
        kafkaProducerService.sendString(message);
        return ResponseEntity.ok("Published Successfully");
    }

    //REQUEST OBJECT
    @PostMapping("/publishJson")
    public ResponseEntity<String> post(@Validated TestRequest request){
        kafkaProducerService.sendRequest(request);
        return ResponseEntity.ok("Published JSON Successfully");
    }

}
