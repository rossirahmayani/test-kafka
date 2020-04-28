package com.rossi.spring.testkafka.exception;

import com.rossi.spring.testkafka.model.KafkaFailedRecordDto;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerAwareErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.serializer.DeserializationException;

import java.time.temporal.ValueRange;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

@Slf4j
public class KafkaErrorHandler implements ContainerAwareErrorHandler {
    public  ThreadLocal<KafkaFailedRecordDto> failureRecord = new ThreadLocal();
    private final BiConsumer<ConsumerRecord<?, ?>, Exception> recovererDLT;
    private final BiConsumer<ConsumerRecord<?, ?>, Exception> recovererDET;
    private final Boolean noRetries;
    private final Integer maxFailures;

    public KafkaErrorHandler(KafkaTemplate<?, ?> template, Integer maxFailures){
        BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> destinationDLTResolver = (cr, e) -> new TopicPartition(cr.topic() + ".fc_core.DLT", cr.partition());
        BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> destinationDETResolver = (cr, e) -> new TopicPartition(cr.topic() + ".fc_core.DET", cr.partition());
        this.maxFailures = maxFailures;
        recovererDLT = new DeadLetterPublishingRecoverer(template, destinationDLTResolver);
        recovererDET = new DeadLetterPublishingRecoverer(template, destinationDETResolver);
        this.noRetries = ValueRange.of(0L, 1L).isValidIntValue((long) maxFailures);
        this.failureRecord.remove();
    }

    @Override
    public void handle(Exception e, List<ConsumerRecord<?, ?>> list, Consumer<?, ?> consumer, MessageListenerContainer messageListenerContainer) {
        if (e instanceof DeserializationException || e instanceof ClassCastException || isDeserializationExceptionOrClassCastExceptionFound(e)) {
            doRecoverDET(list, e, consumer);
        } else {
            if (!doSeeks(list, consumer, e, true)) {
                throw new KafkaException("Seek to current after exception", e);
            }
        }
    }

    public boolean isDeserializationExceptionOrClassCastExceptionFound(Exception e) {
        Throwable rootCause = e.getCause();
        boolean isDeserializationExceptionOrClassCastException = rootCause instanceof DeserializationException || rootCause instanceof ClassCastException;
        while (!isDeserializationExceptionOrClassCastException && rootCause.getCause() != null) { isDeserializationExceptionOrClassCastException = rootCause.getCause() instanceof DeserializationException || rootCause.getCause() instanceof ClassCastException; }
        return isDeserializationExceptionOrClassCastException;
    }

    public boolean doRecover(ConsumerRecord<?, ?> record, Exception exception) {
        if (this.noRetries) {
            return acceptDlt(record, exception);
        } else {
            KafkaFailedRecordDto failedRecord = this.failureRecord.get();
            if (this.maxFailures > 0 && (failedRecord == null || this.newFailure(record, failedRecord))) {
                this.failureRecord.set(KafkaFailedRecordDto.builder().topic(record.topic()).partition(record.partition())
                        .offset(record.offset()).count(1).build());
                return false;
            } else if (this.maxFailures > 0 && failedRecord.doIncrementAndGet() >= this.maxFailures) { return acceptDlt(record, exception);
            } else { return false;
            }
        }
    }

    public void doRecoverDET(List<ConsumerRecord<?, ?>> records, Exception exception, Consumer<?, ?> consumer) {
        Map<TopicPartition, ConsumerRecord<?, ?>> partitions = new LinkedHashMap();
        records.forEach(record -> partitions.computeIfAbsent(new TopicPartition(record.topic(), record.partition()), tp -> record));
        partitions.forEach((tp, cr) -> {
            log.error("DeserializationException exception with topic={}, partition={}, offset {}, " +
                            "exception: {}", cr.topic(), cr.partition(), cr.offset(),
                    exception.getLocalizedMessage());
            recovererDET.accept(cr, exception);
            consumer.commitSync(); });
    }

    public boolean doSeeks(List<ConsumerRecord<?, ?>> records, Consumer<?, ?> consumer, Exception exception, boolean recoverable) {
        Map<TopicPartition, Long> partitions = new LinkedHashMap();
        AtomicBoolean first = new AtomicBoolean(true);
        AtomicBoolean skipped = new AtomicBoolean();
        records.forEach(record -> {
            if (recoverable && first.get()) {
                skipped.set(doRecover(record, exception));
                log.debug("Skipping seek of: " + record);
            }
            if (!recoverable || !first.get() || !skipped.get()) {
                partitions.computeIfAbsent(new TopicPartition(record.topic(), record.partition()),
                        tp -> record.offset());
            }
            first.set(false);
        });
        partitions.forEach(consumer::seek);
        return skipped.get();
    }

    public boolean newFailure(ConsumerRecord<?, ?> record, KafkaFailedRecordDto failedRecordDTO) {
        return !failedRecordDTO.getTopic().equals(record.topic()) || failedRecordDTO.getPartition() != record.partition() || failedRecordDTO.getOffset() != record.offset();
    }

    @Override
    public void clearThreadState() {
        this.failureRecord.remove();
    }

    public boolean acceptDlt(ConsumerRecord<?, ?> record, Exception exception){
        this.recovererDLT.accept(record, exception);
        return true;
    }
}
