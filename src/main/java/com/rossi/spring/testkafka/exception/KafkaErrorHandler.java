package com.rossi.spring.testkafka.exception;

import com.google.gson.JsonSyntaxException;
import com.rossi.spring.testkafka.model.KafkaFailedRecordDto;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerAwareErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.support.serializer.DeserializationException;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

@Slf4j
public class KafkaErrorHandler implements ContainerAwareErrorHandler {
    private static final ThreadLocal<KafkaFailedRecordDto> failureRecord = new ThreadLocal();
    private final BiConsumer<ConsumerRecord<?, ?>, Exception> recovererDLT;

    public KafkaErrorHandler(KafkaTemplate<?, ?> template){
        BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> destinationDLTResolver = (cr, e) -> new TopicPartition(cr.topic() +"-dlt", cr.partition());
        recovererDLT = new DeadLetterPublishingRecoverer(template, destinationDLTResolver);
        failureRecord.remove();
    }

    @Override
    public void handle(Exception e, List<ConsumerRecord<?, ?>> records, Consumer<?, ?> consumer, MessageListenerContainer messageListenerContainer){
        log.warn("Error: {}", e.getMessage());

        // async
        seek().handle(e, records, consumer, messageListenerContainer);
        // sync
        doRecoverDlt(records, e, consumer);
    }

    private void doRecoverDlt(List<ConsumerRecord<?, ?>> records, Exception exception, Consumer<?, ?> consumer) {
        log.info("RECOVER DLT");
        Map<TopicPartition, ConsumerRecord<?, ?>> partitions = new LinkedHashMap();
        records.forEach(record -> partitions.computeIfAbsent(new TopicPartition(record.topic(), record.partition()), tp -> record));
        partitions.forEach((tp, cr) -> {
            recovererDLT.accept(cr, exception);
            consumer.commitSync();
        });
    }

    @Override
    public void clearThreadState() {
        failureRecord.remove();
    }

    private SeekToCurrentErrorHandler seek(){
        SeekToCurrentErrorHandler seek = new SeekToCurrentErrorHandler(recovererDLT);
        seek.addNotRetryableException(DeserializationException.class);
        seek.addNotRetryableException(NullPointerException.class);
        seek.addNotRetryableException(ClassCastException.class);
        seek.addNotRetryableException(IllegalArgumentException.class);
        seek.addNotRetryableException(JsonSyntaxException.class);
        return seek;
    }
}
