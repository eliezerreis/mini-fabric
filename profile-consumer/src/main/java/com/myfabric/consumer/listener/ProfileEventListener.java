package com.myfabric.consumer.listener;

import com.myfabric.consumer.processor.ProfileEventProcessor;
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

    @RetryableTopic(
            attempts = "3",
            backOff = @BackOff(delay = 2000, multiplier = 2.0),
            autoCreateTopics = "true",
            dltTopicSuffix = ".dlq",
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
            include = {RuntimeException.class}
    )
    @KafkaListener(topics = "${fabric.kafka.topic.profile-events:profile.events}")
    public void onProfileEvent(
            ConsumerRecord<String, ProfileEvent> record,
            Acknowledgment ack,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        String profileId = record.key();
        ProfileEvent event = record.value();

        log.info("Received profileId={} type={} partition={} offset={} count={}",
                profileId, event.getEventType(), partition, offset,
                processedCount.incrementAndGet());

        simulateSlow();

        processor.process(event);
        ack.acknowledge();
    }

    @DltHandler
    public void onDlt(ConsumerRecord<String, ?> record,
                      @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                      @Header(value = KafkaHeaders.DLT_ORIGINAL_OFFSET, required = false) Long originalOffset,
                      @Header(value = KafkaHeaders.DLT_EXCEPTION_MESSAGE, required = false) String exceptionMessage) {
        log.error("DLT: profileId={} originalTopic={} originalOffset={} error={}",
                record.key(), topic, originalOffset, exceptionMessage);
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        log.warn("REBALANCE: partitions ASSIGNED → {}", assignments.keySet());
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        log.warn("REBALANCE: partitions REVOKED → {}", partitions);
    }

    @Override
    public void onIdleContainer(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        log.debug("Consumer idle on partitions: {}", assignments.keySet());
    }

    private void simulateSlow() {
        if (simulateSlowMs > 0) {
            try {
                Thread.sleep(simulateSlowMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
