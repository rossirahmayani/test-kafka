package com.rossi.spring.testkafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
public class TestKafkaApplication {

    public static void main(String[] args) {
        SpringApplication.run(TestKafkaApplication.class, args);
    }

}
