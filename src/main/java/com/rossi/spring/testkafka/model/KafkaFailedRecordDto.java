package com.rossi.spring.testkafka.model;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
@Builder
public class KafkaFailedRecordDto {
    private String topic;
    private int partition;
    private long offset;
    private int count;

    public int doIncrementAndGet() {
        return ++this.count;
    }
}
