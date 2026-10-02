package com.snet.transcriptprocessing.repository;

import com.snet.transcriptprocessing.model.TelephonyEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface TelephonyEventRepository extends JpaRepository<TelephonyEvent, Long> {
}
