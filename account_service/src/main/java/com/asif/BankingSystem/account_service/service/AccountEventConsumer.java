package com.asif.BankingSystem.account_service.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class AccountEventConsumer {

    private final AccountService accountService;

    /**
     * Consume transaction.completed event from Kafka -- Produced by Transaction Service
     * No Fraud deducted - credit the amount to the receiver account
     * @param payload
     */
    @KafkaListener(topics = "transaction.completed")
    public void consumeTransactionCompleted(@Payload Map<String, Object> payload) {
        log.info("Received transaction completed event");
        try{
            String receiverAccount = payload.get("receiverAccountNumber").toString();
            BigDecimal amount = (BigDecimal) payload.get("amount");

            log.info("Crediting the amount: {}, to the account: {}", amount, receiverAccount);
            accountService.creditAmount(receiverAccount, amount);

        } catch(Exception ex){
            log.error("Error occurred while crediting to the account. {}", ex.getMessage());
        }
    }

    /**
     * Consume fraud.detected event from Kafka -- Produced by Fraud Detection service
     * Blocks the flagged account
     * @param payload
     */
    @KafkaListener(topics = "fraud.detected")
    public void consumeFraudDetected(@Payload Map<String, Object> payload){
        log.info("Received Fraud Detected event");
        try{
            String accountNumber = payload.get("senderAccountNumber").toString();
            log.info("Fraud detected, Blocking the account: {}", accountNumber);

            accountService.blockAccount(accountNumber);
        } catch(Exception ex){
            log.error("Error occurred while blocking the account. {}", ex.getMessage());
        }
    }

}
