package com.asif.BankingSystem.transaction_service.service;

import com.asif.BankingSystem.transaction_service.entity.Transaction;
import com.asif.BankingSystem.transaction_service.entity.TransactionStatus;
import com.asif.BankingSystem.transaction_service.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class TransactionEventConsumer {

    private final TransactionService transactionService;
    private final TransactionRepository transactionRepository;

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final RedisTemplate<String, String> redisTemplate;

    private final static long OTP_EXPIRY_MINUTES = 5;
    private final static String TRANSACTION_OTP_GENERATED_TOPIC = "transaction.otp.generated";

    /**
     * consume verification.required event from kafka -- produced by fraud detection service
     * Generate OTP and ask user to verify.
     * @param payload
     */
    @KafkaListener(topics = "verification.required")
    public void verificationRequired(@Payload Map<String, Object> payload){

        try{
            String transactionId = payload.get("transactionId").toString();
            String accountNumber = payload.get("accountNumber").toString();
            String reason = payload.get("reason").toString();

            log.info("Verification required - transaction: {}, account: {}, reason: {}",
                                    transactionId, accountNumber, reason);

            Transaction transaction = transactionRepository.findById(transactionId)
                    .orElseThrow(() -> new RuntimeException("Transaction not found - "+transactionId));

            if(transaction.getStatus() != TransactionStatus.PROCESSING){
                log.warn("Transaction: {}, is not in PROCESSING status, skipping the transaction.", transactionId);
                return;
            }

            //Generate OTP
            String otp = String.format("%06d", (int) (Math.random() * 900000) + 100000);

            //Store OTP in Redis - expire in 5 minutes
            String key = "verification:otp_"+transactionId;
            redisTemplate.opsForValue().set(key, otp, OTP_EXPIRY_MINUTES, TimeUnit.MINUTES);

            //update status
            transaction.setStatus(TransactionStatus.PENDING_VERIFICATION);
            transactionRepository.save(transaction);

            log.info("OTP generated for the transaction: {}, expires in {} minutes", transactionId, OTP_EXPIRY_MINUTES);

            //Notify user
            Map<String,Object> otpEvent = new HashMap<>();
            otpEvent.put("transactionId", transactionId);
            otpEvent.put("accountNumber", accountNumber);
            otpEvent.put("reason", reason);
            otpEvent.put("otp", otp);
            otpEvent.put("amount", payload.get("amount"));

            kafkaTemplate.send(TRANSACTION_OTP_GENERATED_TOPIC, transactionId, otpEvent);

        }catch (Exception e){
            log.error("Error occurred while Transaction verification: {}",e.getMessage());
        }

    }


    @KafkaListener(topics = "fraud.check.clean")
    public void consumeFraudCheckCleanResult(@Payload Map<String, Object> payload){
        try{
            String transactionId = payload.get("transactionId").toString();
            transactionService.processFraudCheckCleanResult(transactionId);
        } catch (Exception e) {
            log.error("Error occurred while processing the fraud check clean result: {}",
                    e.getMessage());
        }
    }
}
