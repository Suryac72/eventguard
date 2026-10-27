package com.eventguard.repository;

import com.eventguard.entity.AnomalyRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;

public interface AnomalyRecordRepository extends JpaRepository<AnomalyRecord, Long> {
    List<AnomalyRecord> findTop100ByOrderByDetectedAtDesc();
}
