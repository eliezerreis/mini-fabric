package com.myfabric.consumer.listener;

import com.myfabric.consumer.processor.ProfileEventProcessor;
import com.myfabric.events.EventType;
import com.myfabric.events.ProfileEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Demonstrates:
 * - Ordering: same profileId key → same partition → sequential delivery
 * - Rebalance: ConsumerSeekAware callbacks log partition assignments/revocations
 * - Retries: @RetryableTopic auto-creates retry topics with exponential backoff
 * - DLQ: after max attempts the message lands on the .dlq topic
 * - Poison messages: ErrorHandlingDeserializer prevents stuck consumer; handled via DLT
 * - Lag: use /simulate/burst then watch Kafka UI at :8090
 */
@Component
public class ProfileEventListener implements ConsumerSeekAware {

    private static final Logger log = LoggerFactory.getLogger(ProfileEventListener.class);

    private final ProfileEventProcessor processor;

    @Value("${fabric.consumer.simulate-slow-ms:0}")
    private long simulateSlowMs;

    private final AtomicLong processedCount = new AtomicLong();

    public ProfileEventListener(ProfileEventProcessor processor) {
        this.processor = processor;
    }

    // ------------------------------------------------------------------ //
    //  Main listener with non-blocking retry + DLQ
    //
    //  Spring Kafka automatically creates:
    //    profile.events-retry-0   (attempt 2, delay 2s)
    //    profile.events-retry-1   (attempt 3, delay 4s)
    //    profile.events.dlq       (after attempt 3)
    // ------------------------------------------------------------------ //

    @RetryableTopic(
            attempts = "3",
            backOff = @BackOff(delay = 2000, multiplier = 2.0),
            autoCreateTopics = "true",
            dltTopicSuffix = ".dlq",
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
            include = {RuntimeException.class}
    )
    @KafkaListener(
            topics = "${fabric.kafka.topic.profile-events:profile.events}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onProfileEvent(
            ConsumerRecord<String, ProfileEvent> record,
            Acknowledgment ack,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        String profileId = record.key();
        ProfileEvent event = record.value();

        log.info("Received profileId={} type={} partition={} offset={} attempt={}",
                profileId, event.getEventType(), partition, offset,
                processedCount.incrementAndGet());

        simulateSlow();

        processor.process(event);
        ack.acknowledge();
        log.debug("Committed offset={} partition={}", offset, partition);
    }

    // ------------------------------------------------------------------ //
    //  DLT handler — runs when all retries are exhausted
    // ------------------------------------------------------------------ //

    @DltHandler
    public void onDlt(ConsumerRecord<String, ?> record,
                      @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                      @Header(KafkaHeaders.ORIGINAL_OFFSET) long originalOffset,
                      @Header(KafkaHeaders.EXCEPTION_MESSAGE) String exceptionMessage) {
        log.error("DLT: profileId={} originalTopic={} originalOffset={} error={}",
                record.key(), topic, originalOffset, exceptionMessage);
        // dlq-consumer service reads the .dlq topic and persists failures to MongoDB
    }

    // ------------------------------------------------------------------ //
    //  Rebalance awareness — ConsumerSeekAware callbacks
    // ------------------------------------------------------------------ //

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        log.warn("REBALANCE: partitions ASSIGNED → {}", assignments.keySet());
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        log.warn("REBALANCE: partitions REVOKED → {} (processing paused during rebalance)", partitions);
    }

    @Override
    public void onIdleContainer(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        log.debug("Consumer idle — no new messages on partitions: {}", assignments.keySet());
    }

    // ------------------------------------------------------------------ //
    //  Lag simulation: set fabric.consumer.simulate-slow-ms > 0 to build lag
    // ------------------------------------------------------------------ //

    private void simulateSlow() {
        if (simulateSlowMs > 0) {
            try {
                log.debug("Simulating slow consumer: sleeping {}ms", simulateSlowMs);
                Thread.sleep(simulateSlowMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
