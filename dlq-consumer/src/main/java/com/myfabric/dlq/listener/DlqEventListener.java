package com.myfabric.dlq.listener;

import com.myfabric.dlq.document.FailedEventDocument;
import com.myfabric.dlq.repository.FailedEventRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

/**
 * Reads all DLQ topics matching profile.events*.dlq.
 * Persists each failed event to MongoDB for manual review/replay.
 *
 * To see DLQ in action:
 *   POST /simulate/poison  →  watch this listener log a FAILED_EVENT
 *   GET  /dlq/events       →  (via DlqController) list all failed events
 */
@Component
public class DlqEventListener {

    private static final Logger log = LoggerFactory.getLogger(DlqEventListener.class);

    private final FailedEventRepository repository;

    public DlqEventListener(FailedEventRepository repository) {
        this.repository = repository;
    }

    @KafkaListener(
            topicPattern = "profile\\.events.*\\.dlq",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "dlqKafkaListenerContainerFactory"
    )
    public void onDlqEvent(
            ConsumerRecord<String, ?> record,
            @Header(value = KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(value = KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(value = KafkaHeaders.OFFSET) long offset,
            @Header(value = KafkaHeaders.EXCEPTION_MESSAGE, required = false) String exceptionMessage,
            @Header(value = KafkaHeaders.EXCEPTION_FQCN, required = false) String exceptionClass) {

        log.error("DLQ event received: topic={} partition={} offset={} profileId={} error={}",
                topic, partition, offset, record.key(), exceptionMessage);

        var doc = new FailedEventDocument();
        doc.setId(UUID.randomUUID().toString());
        doc.setProfileId(record.key());
        doc.setTopic(topic);
        doc.setPartition(partition);
        doc.setOffset(offset);
        doc.setExceptionClass(exceptionClass);
        doc.setExceptionMessage(exceptionMessage);
        doc.setRawPayload(record.value() != null ? record.value().toString() : "<null>");
        doc.setReceivedAt(Instant.now());
        doc.setStatus("PENDING_REVIEW");

        repository.save(doc);
        log.info("Persisted failed event to MongoDB id={}", doc.getId());
    }
}
