package com.eventguard.controller;

import com.eventguard.repository.AnomalyRecordRepository;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/anomalies")
public class AnomalyController {
    private final AnomalyRecordRepository repository;

    public AnomalyController(AnomalyRecordRepository repository) {
        this.repository = repository;
    }

    @GetMapping
    public Object recent() {
        return repository.findTop100ByOrderByDetectedAtDesc();
    }
}
