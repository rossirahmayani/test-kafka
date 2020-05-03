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
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.util.backoff.FixedBackOff;

import java.time.temporal.ValueRange;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Predicate;

@Slf4j
public class KafkaErrorHandler implements ContainerAwareErrorHandler {
    private ThreadLocal<KafkaFailedRecordDto> failureRecord = new ThreadLocal();
    private final BiConsumer<ConsumerRecord<?, ?>, Exception> recovererDLT;
    private final BiConsumer<ConsumerRecord<?, ?>, Exception> recovererDET;
    private final Boolean noRetries;
    private final Integer maxFailures;

    public KafkaErrorHandler(KafkaTemplate<?, ?> template, Integer maxFailures){
        BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> destinationDLTResolver = (cr, e) -> new TopicPartition(cr.topic() + ".DLT", cr.partition());
        BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> destinationDETResolver = (cr, e) -> new TopicPartition(cr.topic() + ".DET", cr.partition());
        this.maxFailures = maxFailures;
        recovererDLT = new DeadLetterPublishingRecoverer(template, destinationDLTResolver);
        recovererDET = new DeadLetterPublishingRecoverer(template, destinationDETResolver);
        this.noRetries = ValueRange.of(0L, 1L).isValidIntValue((long) maxFailures);
        this.failureRecord.remove();
    }

    @Override
    public void handle(Exception e, List<ConsumerRecord<?, ?>> records, Consumer<?, ?> consumer, MessageListenerContainer messageListenerContainer){
        Optional.of(e)
                .filter(x -> x instanceof DeserializationException || x instanceof ClassCastException || isDeserializationExceptionOrClassCastExceptionFound(x))
                .ifPresentOrElse(c -> doRecoverDET(records, c, consumer),
                        () -> Optional.of(e).
                                filter(x -> !doSeeks(records, consumer, x, true))
                                .ifPresent(c -> {throw new KafkaException("Seek to current after exception", c);}));

    }

    private void doRecoverDET(List<ConsumerRecord<?, ?>> records, Exception exception, Consumer<?, ?> consumer) {
        log.info("RECOVER DET");
        Map<TopicPartition, ConsumerRecord<?, ?>> partitions = new LinkedHashMap();
        records.forEach(record -> partitions.computeIfAbsent(new TopicPartition(record.topic(), record.partition()), tp -> record));
        partitions.forEach((tp, cr) -> {
            log.info("DeserializationException exception with topic={}, partition={}, offset {}, " +
                            "exception: {}", cr.topic(), cr.partition(), cr.offset(),
                    exception.getLocalizedMessage());
            recovererDET.accept(cr, exception);
            consumer.commitSync(); });
    }

    private boolean doSeeks(List<ConsumerRecord<?, ?>> records, Consumer<?, ?> consumer, Exception exception, boolean recoverable){
        Map<TopicPartition, Long> partitions = new LinkedHashMap();
        AtomicBoolean first = new AtomicBoolean(true);
        AtomicBoolean skipped = new AtomicBoolean();
        records.forEach(record -> {
            Optional.ofNullable(record)
                    .filter(r -> recoverable && first.get())
                    .ifPresent(c -> skipped.set(doRecover(c, exception)));
            Optional.ofNullable(record)
                    .filter(r -> !recoverable || !first.get() || !skipped.get())
                    .ifPresent(c -> partitions.computeIfAbsent(new TopicPartition(c.topic(), c.partition()), tp -> c.offset()));
            first.set(false);
        });
        partitions.forEach(consumer::seek);
        return skipped.get();
    }

    private boolean doRecover(ConsumerRecord<?, ?> record, Exception exception){
        KafkaFailedRecordDto failedRecord = this.failureRecord.get();
        return Optional.ofNullable(failedRecord)
                .filter(f -> this.noRetries || (this.maxFailures > 0 && failedRecord.doIncrementAndGet() >= this.maxFailures) )
                .map(c -> acceptDlt(record, exception))
                .orElseGet(() -> setFailureRecord(record, failedRecord));
    }

    private boolean acceptDlt(ConsumerRecord<?, ?> record, Exception exception){
        log.info("RECOVER DLT");
        this.recovererDLT.accept(record, exception);
        return true;
    }

    private boolean isDeserializationExceptionOrClassCastExceptionFound(Exception e) {
        Throwable rootCause = e.getCause();
        Boolean isDeserializationExceptionOrClassCastException = rootCause instanceof DeserializationException || rootCause instanceof ClassCastException;
        while (!isDeserializationExceptionOrClassCastException && rootCause.getCause() != null) { isDeserializationExceptionOrClassCastException = rootCause.getCause() instanceof DeserializationException || rootCause.getCause() instanceof ClassCastException; }
        return isDeserializationExceptionOrClassCastException;
    }

    private boolean newFailure(ConsumerRecord<?, ?> record, KafkaFailedRecordDto failedRecordDTO) {
        return !failedRecordDTO.getTopic().equals(record.topic()) || failedRecordDTO.getPartition() != record.partition() || failedRecordDTO.getOffset() != record.offset();
    }

    private boolean setFailureRecord(ConsumerRecord<?, ?> record, KafkaFailedRecordDto failedRecordDto){
        Predicate<KafkaFailedRecordDto> check = c -> (c == null || this.newFailure(record, c));
        Optional.of(check.test(failedRecordDto))
                .ifPresent(f -> this.failureRecord.set(KafkaFailedRecordDto.builder().topic(record.topic()).partition(record.partition()).offset(record.offset()).count(1).build()));
        return false;
    }

    @Override
    public void clearThreadState() {
        this.failureRecord.remove();
    }

}
