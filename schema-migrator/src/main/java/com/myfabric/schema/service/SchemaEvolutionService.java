package com.myfabric.schema.service;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

@Service
public class SchemaEvolutionService {

    private static final Logger log = LoggerFactory.getLogger(SchemaEvolutionService.class);

    private static final String SUBJECT = "profile.events-value";

    private final SchemaRegistryClient schemaRegistryClient;

    public SchemaEvolutionService(SchemaRegistryClient schemaRegistryClient) {
        this.schemaRegistryClient = schemaRegistryClient;
    }

    public Map<String, Object> registerV1() throws IOException, RestClientException {
        return register("ProfileEvent-v1.avsc", "v1");
    }

    public Map<String, Object> registerV2() throws IOException, RestClientException {
        return register("ProfileEvent-v2.avsc", "v2");
    }

    public Map<String, Object> getCompatibilityStatus() throws IOException, RestClientException {
        var compatibility = schemaRegistryClient.getCompatibility(SUBJECT);
        var versions = schemaRegistryClient.getAllVersions(SUBJECT);
        return Map.of(
                "subject", SUBJECT,
                "compatibilityMode", compatibility,
                "versions", versions
        );
    }

    public Map<String, Object> checkV2CompatibleWithV1() throws IOException, RestClientException {
        String schemaStr = loadSchema("ProfileEvent-v2.avsc");
        Schema parsed = new Schema.Parser().parse(schemaStr);
        boolean isCompatible = schemaRegistryClient.testCompatibility(SUBJECT, new AvroSchema(parsed));
        return Map.of(
                "subject", SUBJECT,
                "v2CompatibleWithExisting", isCompatible,
                "explanation", isCompatible
                        ? "v2 is BACKWARD compatible: v2 consumers can read v1 messages"
                        : "INCOMPATIBLE: would break existing consumers or producers"
        );
    }

    private Map<String, Object> register(String resourceName, String label) throws IOException, RestClientException {
        String schemaStr = loadSchema(resourceName);
        Schema parsed = new Schema.Parser().parse(schemaStr);
        int id = schemaRegistryClient.register(SUBJECT, new AvroSchema(parsed));
        log.info("Registered schema {} → subject={} id={}", label, SUBJECT, id);
        return Map.of("subject", SUBJECT, "schemaId", id, "version", label);
    }

    private String loadSchema(String name) throws IOException {
        return new ClassPathResource("avro/" + name)
                .getContentAsString(StandardCharsets.UTF_8);
    }

    public List<Integer> listVersions() throws IOException, RestClientException {
        return schemaRegistryClient.getAllVersions(SUBJECT);
    }
}
