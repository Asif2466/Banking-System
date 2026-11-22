package com.asif.BankingSystem.fraudDetection_service.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class FraudDetectionEventConsumer {

    private final FraudDetectionService fraudDetectionService;

    /**
     * Consume transaction.completed event from Kafka -- Produced by Transaction Service
     * Every tranaction should go through fraud check before completing
     * @param payload
     */
    @KafkaListener(topics = "transaction.initiated", groupId = "fraud-detection-group")
    public void consumeTransactionInitiated(@Payload Map<String, Object> payload) {
        log.info("Received Transaction Initiated event");
        try{
            fraudDetectionService.checkTransaction(payload);
        } catch (Exception ex){
            log.error("Error occurred while transaction initiation in fraud detection: {}",
                    ex.getMessage());
        }
    }
}
