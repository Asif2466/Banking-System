package com.asif.BankingSystem.fraudDetection_service.service;

import com.asif.BankingSystem.fraudDetection_service.client.AccountServiceClient;
import com.asif.BankingSystem.fraudDetection_service.model.FraudCheckResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class FraudDetectionService {

    private final AccountServiceClient accountServiceClient;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final RedisTemplate<String, String> redisTemplate;

    @Value("${fraud.max-transaction-per-minute}")
    private int maxTransactionPerMinute;

    @Value("${fraud.suspicious-amount-multiplier}")
    private int suspiciousAmountMultiplier;

    @Value("${fraud.max-balance-percentage}")
    private double maxBalancePercentage;

    private final static String VERIFICATION_REQUIRED_TOPIC = "verification.required";
    private final static String FRAUD_CHECK_CLEAN_RESULT_TOPIC = "fraud.check.clean";

    public void checkTransaction(Map<String, Object> payload) {
        String transactionId = (String) payload.get("transactionId");
        String accountNumber = (String) payload.get("senderAccountNumber");
        BigDecimal amount = (BigDecimal) payload.get("amount");

        //Fetch real balance from account service
        BigDecimal senderBalance = accountServiceClient.getAccountBalance(accountNumber);

        log.info("Checking transaction:{}, account: {}, amount: {}, balance: {}",
                transactionId, accountNumber, amount, senderBalance);

        FraudCheckResult result = performFraudChecks(accountNumber, amount, senderBalance);

        if(result.isFraud()){
            log.info("Suspicious activity detected - account: {}, " +
                    "reason: {} - requesting OTP verification",
                    accountNumber, result.getReason());

            Map<String, Object> verificationEvent = new HashMap<>();
            verificationEvent.put("transactionId", transactionId);
            verificationEvent.put("accountNumber", accountNumber);
            verificationEvent.put("amount", amount);
            verificationEvent.put("reason", result.getReason());

            kafkaTemplate.send(VERIFICATION_REQUIRED_TOPIC, transactionId, verificationEvent);

        } else {
            log.info("Transaction clean");

            Map<String, Object> transactionCleanEvent = new HashMap<>();
            transactionCleanEvent.put("transactionId", transactionId);
            transactionCleanEvent.put("isFraud", false);
            transactionCleanEvent.put("reason", null);

            kafkaTemplate.send(FRAUD_CHECK_CLEAN_RESULT_TOPIC, transactionId, transactionCleanEvent);
        }
    }


    private FraudCheckResult performFraudChecks(
            String accountNumber, BigDecimal amount, BigDecimal senderBalance) {

        // Pattern: 1 -> Velocity check
        if(isVelocityExceeded(accountNumber)){
            return FraudCheckResult.builder()
                    .fraud(true)
                    .reason("Too many transaction in 60 seconds - velocity limit exceeded")
                    .build();
        }

        // Pattern: 2 -> Average amount check
        if(isAverageAmountSuspicious(accountNumber, amount)){
            return FraudCheckResult.builder()
                    .fraud(true)
                    .reason("Unusual transaction amount - exceeds 3x of your average")
                    .build();
        }

        // Pattern: 3 -> Balance check
        if(senderBalance.compareTo(BigDecimal.ZERO) > 0
                && isBalanceCheckFailed(senderBalance, amount)){
            return FraudCheckResult.builder()
                    .fraud(true)
                    .reason("Transaction exceeds 90% of your account balance")
                    .build();
        }

        return FraudCheckResult.builder()
                .fraud(false)
                .reason(null)
                .build();
    }


    private boolean isVelocityExceeded(String accountNumber) {
        String key = "fraud:velocity_"+accountNumber;
        Long count = redisTemplate.opsForValue().increment(key);

        if(count != null && count == 1){
            redisTemplate.expire(key, 60, TimeUnit.SECONDS);
        }

        log.info("Velocity check - account: {}, count : {}/{}",
                accountNumber, count, maxTransactionPerMinute);

        return count != null && count > maxTransactionPerMinute;
    }


    private boolean isAverageAmountSuspicious(String accountNumber, BigDecimal amount) {
        String avgKey = "fraud:avgAmt_"+accountNumber;
        String avgStr = redisTemplate.opsForValue().get(avgKey);

        if(avgStr == null){
            redisTemplate.opsForValue().set(avgKey, amount.toString());
            return false;
        }

        BigDecimal avgAmount = new BigDecimal(avgStr);
        BigDecimal threshold = avgAmount.multiply(BigDecimal.valueOf(suspiciousAmountMultiplier));

        //update running average
        BigDecimal newAvg = avgAmount.add(amount)
                .divide(BigDecimal.valueOf(2), 2, RoundingMode.HALF_UP);

        redisTemplate.opsForValue().set(avgKey, newAvg.toString());

        log.info("Average amount check - amount: {}, threshold: {}, suspicious: {}",
                                                amount, threshold, amount.compareTo(threshold) > 0);

        return amount.compareTo(threshold) > 0;
    }


    private boolean isBalanceCheckFailed(BigDecimal senderBalance, BigDecimal amount) {
        BigDecimal maxAllowed = senderBalance.multiply(BigDecimal.valueOf(maxBalancePercentage));

        log.info("Balance check - amount: {}, maxAllowed: {}, suspicious: {}",
                                    amount, maxAllowed, amount.compareTo(maxAllowed) > 0);

        return amount.compareTo(maxAllowed) > 0;
    }

}
