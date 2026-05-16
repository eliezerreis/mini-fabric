package com.myfabric.dlq.listener;

import com.myfabric.dlq.service.FailedEventService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/**
 * Reads all DLQ topics matching profile.events*.dlq.
 * Persists each failed event to MongoDB for manual review/replay.
 *
 * To see DLQ in action:
 *   POST /simulate/poison  →  watch this listener log a FAILED_EVENT
 *   GET  /dlq/events       →  (via DlqController) list all failed events
 */
@Slf4j
@Component
public class DlqEventListener {

    private final FailedEventService failedEventService;

    public DlqEventListener(FailedEventService failedEventService) {
        this.failedEventService = failedEventService;
    }

    @KafkaListener(
            topicPattern = "profile\\.events.*\\.dlq",
            groupId = "${spring.kafka.consumer.group-id}"
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

        failedEventService.persist(record.key(), topic, partition, offset,
                exceptionClass, exceptionMessage,
                record.value() != null ? record.value().toString() : "<null>");
    }
}
