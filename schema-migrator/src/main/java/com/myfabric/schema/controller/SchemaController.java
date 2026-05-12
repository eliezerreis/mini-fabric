package com.myfabric.schema.controller;

import com.myfabric.schema.service.SchemaEvolutionService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * Schema evolution demo endpoints.
 *
 * Typical demo flow:
 *   1. POST /schemas/v1/register   → register v1
 *   2. POST /schemas/v2/check      → verify v2 is backward-compatible before promoting
 *   3. POST /schemas/v2/register   → register v2 (producers and consumers upgrade independently)
 *   4. GET  /schemas/versions      → list registered versions
 *   5. GET  /schemas/compatibility → show current compatibility mode
 */
@RestController
@RequestMapping("/schemas")
public class SchemaController {

    private final SchemaEvolutionService service;

    public SchemaController(SchemaEvolutionService service) {
        this.service = service;
    }

    @PostMapping("/v1/register")
    public ResponseEntity<Map<String, Object>> registerV1() throws Exception {
        return ResponseEntity.ok(service.registerV1());
    }

    @PostMapping("/v2/register")
    public ResponseEntity<Map<String, Object>> registerV2() throws Exception {
        return ResponseEntity.ok(service.registerV2());
    }

    @PostMapping("/v2/check")
    public ResponseEntity<Map<String, Object>> checkV2Compatibility() throws Exception {
        return ResponseEntity.ok(service.checkV2CompatibleWithV1());
    }

    @GetMapping("/versions")
    public ResponseEntity<List<Integer>> listVersions() throws Exception {
        return ResponseEntity.ok(service.listVersions());
    }

    @GetMapping("/compatibility")
    public ResponseEntity<Map<String, Object>> compatibilityStatus() throws Exception {
        return ResponseEntity.ok(service.getCompatibilityStatus());
    }
}
