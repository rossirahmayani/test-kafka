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
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Predicate;

@Slf4j
public class KafkaErrorHandler implements ContainerAwareErrorHandler {
    private static final ThreadLocal<KafkaFailedRecordDto> failureRecord = new ThreadLocal();
    private final BiConsumer<ConsumerRecord<?, ?>, Exception> recovererDLT;
    private final BiConsumer<ConsumerRecord<?, ?>, Exception> recovererDET;
    private final Boolean noRetries;
    private final Integer maxFailures;

    public KafkaErrorHandler(KafkaTemplate<?, ?> template, Integer maxFailures){
        BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> destinationDLTResolver = (cr, e) -> new TopicPartition(cr.topic() +".DLT", cr.partition());
        BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> destinationDETResolver = (cr, e) -> new TopicPartition(cr.topic() +".DET", cr.partition());
        this.maxFailures = maxFailures;
        recovererDLT = new DeadLetterPublishingRecoverer(template, destinationDLTResolver);
        recovererDET = new DeadLetterPublishingRecoverer(template, destinationDETResolver);
        this.noRetries = ValueRange.of(0L, 1L).isValidIntValue((long) maxFailures);
        failureRecord.remove();
    }

    @Override
    public void handle(Exception e, List<ConsumerRecord<?, ?>> records, Consumer<?, ?> consumer, MessageListenerContainer messageListenerContainer){
        Predicate<Exception> checkDet = x -> x instanceof DeserializationException;
        Predicate<Exception> checkClassCast = x -> x instanceof ClassCastException;
        Predicate<Exception> checkOtherDet = this::isDeserializationExceptionOrClassCastExceptionFound;
        Optional.of(e)
                .filter(x -> checkDet.or(checkClassCast).or(checkOtherDet).test(x))
                .ifPresentOrElse(c -> doRecoverDET(records, c, consumer),
                        () -> Optional.of(e)
                                .filter(x -> !doSeeks(records, consumer, x, true))
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
        Predicate<ConsumerRecord<?,?>> checkRecover = r -> recoverable;
        Predicate<ConsumerRecord<?,?>> checkFirst = r -> first.get();
        Predicate<ConsumerRecord<?,?>> checkSkippedNegate = r -> !skipped.get();
        records.forEach(record -> {
            Optional.ofNullable(record)
                    .filter(r -> checkRecover.and(checkFirst).test(r))
                    .ifPresent(c -> skipped.set(doRecover(c, exception)));
            Optional.ofNullable(record)
                    .filter(r -> checkRecover.negate().or(checkFirst.negate()).or(checkSkippedNegate).test(r))
                    .ifPresent(c -> partitions.computeIfAbsent(new TopicPartition(c.topic(), c.partition()), tp -> c.offset()));
            first.set(false);
        });
        partitions.forEach(consumer::seek);
        return skipped.get();
    }

    public boolean doRecover(ConsumerRecord<?, ?> record, Exception exception){
        Predicate<KafkaFailedRecordDto> checkRetry = f -> this.noRetries;
        KafkaFailedRecordDto failedRecord = Optional.ofNullable(failureRecord.get()).orElse(null);
        boolean checkMaxFail = checkMaxFail(failedRecord) ;
        return Optional.ofNullable(failedRecord)
                .filter(checkRetry)
                .map(c -> acceptDlt(record, exception))
                .orElseGet(() ->
                        Optional.ofNullable(failedRecord).filter(f -> checkMaxFail)
                                .map(c -> acceptDlt(record, exception))
                                .orElseGet(() -> setFailureRecord(record, Optional.ofNullable(failureRecord.get()).orElse(null)))
                );
    }

    public boolean acceptDlt(ConsumerRecord<?, ?> record, Exception exception){
        log.info("RECOVER DLT");
        this.recovererDLT.accept(record, exception);
        return true;
    }

    public boolean isDeserializationExceptionOrClassCastExceptionFound(Exception e) {
        Throwable rootCause = e.getCause();
        Predicate<Throwable> getDet = x -> x instanceof DeserializationException;
        Predicate<Throwable> getClassCast = x -> x instanceof ClassCastException;

        boolean isDeserializationExceptionOrClassCastException = getDet.or(getClassCast).test(rootCause);
        while (!isDeserializationExceptionOrClassCastException && Optional.ofNullable(rootCause.getCause()).isPresent()){ isDeserializationExceptionOrClassCastException = getDet.or(getClassCast).test(rootCause.getCause()); }
        return isDeserializationExceptionOrClassCastException;
    }

    public boolean newFailure(ConsumerRecord<?, ?> record, KafkaFailedRecordDto failedRecordDTO) {
        Predicate<KafkaFailedRecordDto> checkTopicDiff = x -> !x.getTopic().equals(record.topic());
        Predicate<KafkaFailedRecordDto> checkPartitionDiff = x -> x.getPartition() != record.partition();
        Predicate<KafkaFailedRecordDto> checkOffsetDiff = x -> x.getOffset() != record.offset();
        return checkTopicDiff.or(checkPartitionDiff).or(checkOffsetDiff).test(failedRecordDTO);
    }

    public boolean setFailureRecord(ConsumerRecord<?, ?> record, KafkaFailedRecordDto failedRecordDto){
        Predicate<KafkaFailedRecordDto> checkNull = Objects::isNull;
        Predicate<KafkaFailedRecordDto> checkNewFailure = c -> this.newFailure(record, c);
        Optional.of(checkNull.or(checkNewFailure).test(failedRecordDto))
                .ifPresent(f -> failureRecord.set(KafkaFailedRecordDto.builder().topic(record.topic()).partition(record.partition()).offset(record.offset()).count(1).build()));
        return false;
    }

    public boolean checkMaxFail(KafkaFailedRecordDto kafkaFailedRecordDto) {
        return Optional.ofNullable(kafkaFailedRecordDto)
                .filter(r -> this.maxFailures > 0)
                .filter(k -> k.doIncrementAndGet() >= this.maxFailures)
                .isPresent();
    }

    @Override
    public void clearThreadState() {
        failureRecord.remove();
    }
}
