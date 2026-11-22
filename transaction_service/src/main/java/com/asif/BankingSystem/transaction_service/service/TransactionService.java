package com.asif.BankingSystem.transaction_service.service;

import com.asif.BankingSystem.transaction_service.client.AccountServiceClient;
import com.asif.BankingSystem.transaction_service.dto.TransactionResponse;
import com.asif.BankingSystem.transaction_service.dto.TransferRequest;
import com.asif.BankingSystem.transaction_service.entity.Transaction;
import com.asif.BankingSystem.transaction_service.entity.TransactionStatus;
import com.asif.BankingSystem.transaction_service.entity.TransactionType;
import com.asif.BankingSystem.transaction_service.event.TransactionCompletedEvent;
import com.asif.BankingSystem.transaction_service.event.TransactionInitiatedEvent;
import com.asif.BankingSystem.transaction_service.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class TransactionService {

    private final TransactionRepository transactionRepository;
    private final AccountServiceClient accountSerivceClient;

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final RedisTemplate<String, String> redisTemplate;

    private final static String TRANSACTION_INITIATED_TOPIC = "transaction.initiated";
    private final static String TRANSACTION_COMPLETED_TOPIC = "transaction.completed";
    private final static String TRANSACTION_REFUNDED_TOPIC = "transaction.refunded";
    private final static String FRAUD_DETECTED_TOPIC = "fraud.detected";

    /**
     * SAGA STEP-1:Initiate transfer
     * 1 -> Deduct amount from sender via feign
     * 2 -> Saves transaction as PROCESSING
     * 3 -> Publish event to Kafka for fraud check
     * @param request
     * @return
     */
    public TransactionResponse transfer(TransferRequest request) {

        log.info("SAGA START - Transfer: from {} -> {}, amount: {}",
                request.senderAccountNumber(),
                request.recipientAccountNumber(),
                request.amount()
                );

        // SAGA STEP - 1 : Deduct amount from Sender
        accountSerivceClient.deductAmount(request.senderAccountNumber(), request.amount());

        // SAGA STEP - 2 : save transaction
        Transaction transaction = Transaction.builder()
                .senderAccountNumber(request.senderAccountNumber())
                .recipientAccountNumber(request.recipientAccountNumber())
                .amount(request.amount())
                .type(TransactionType.TRANSFER)
                .status(TransactionStatus.PROCESSING)
                .description(request.description())
                .referenceNumber(UUID.randomUUID().toString())
                .build();
        Transaction savedTransaction = transactionRepository.save(transaction);
        log.info("Transaction saved as PROCESSING: {}", savedTransaction.getId());

        // SAGA STEP - 3 : Publish event to Kafka for fraud check
        TransactionInitiatedEvent event = TransactionInitiatedEvent.builder()
                .transactionId(savedTransaction.getId())
                .senderAccountNumber(savedTransaction.getSenderAccountNumber())
                .receiverAccountNumber(savedTransaction.getRecipientAccountNumber())
                .amount(savedTransaction.getAmount())
                .description(savedTransaction.getDescription())
                .build();
        kafkaTemplate.send(TRANSACTION_INITIATED_TOPIC, savedTransaction.getId(), event);
        log.info("TransactionInitiatedEvent published: {}", savedTransaction.getId());

        return mapToResponse(savedTransaction);
    }


    public TransactionResponse getTransaction(String transactionId) {
        return mapToResponse(
                transactionRepository.findById(transactionId)
                .orElseThrow(() -> new RuntimeException("Transaction not found")));
    }


    public List<TransactionResponse> getTransactionHistory(String accountNumber) {
        return transactionRepository.findBySenderAccountNumberOrderByCreatedAtDesc(accountNumber)
                .stream()
                .map(this::mapToResponse) //transaction -> mapToResponse(transaction)
                .collect(Collectors.toList());
    }


    public TransactionResponse verifyOTP(String transactionId, String otp) {
        log.info("OTP verification for the transaction: {}", transactionId);

        Transaction transaction = transactionRepository.findById(transactionId)
                .orElseThrow(() -> new RuntimeException("Transaction not found"));

        String otpKey = "verification:otp_"+transactionId;
        String storedOtp = redisTemplate.opsForValue().get(otpKey);

        if(storedOtp == null){
            log.warn("OTP expired for the transaction: {}", transactionId);
            compensateTransaction(transaction, "OTP Expired - transaction cancelled and amount refunded to sender.");
            return mapToResponse(transaction);
        }

        if(!storedOtp.equals(otp)){
            log.warn("Wrong OTP - blocking the account: {}, transaction: {} cancelled and amount refunded to sender",
                    transaction.getSenderAccountNumber(), transactionId);
            redisTemplate.delete(otpKey);
            blockAccountAndCompensateTransaction(transaction,
                    "Wrong OTP entered - transaction cancelled"+
                    "account blocked temporarily for security purpose.");

            return mapToResponse(transaction);
        }

        //valid OTP - complete transaction
        log.info("OTP verified successfully for the transaction: {}", transactionId);
        redisTemplate.delete(otpKey);
        completeTransaction(transaction);
        return mapToResponse(transaction);
    }


    private void compensateTransaction(Transaction transaction, String reason) {
        log.warn("SAGA COMPENSATION - amount: {}, refunding to the account: {}",
                transaction.getAmount(), transaction.getSenderAccountNumber());

        accountSerivceClient.creditAmount(transaction.getSenderAccountNumber(), transaction.getAmount());

        transaction.setStatus(TransactionStatus.FLAGGED);
        transaction.setFailureReason(reason +
                " - SAGA Compensation executed, amount refunded at "+ LocalDateTime.now());

        transactionRepository.save(transaction);

        //PUBLISH refund event to Notification service to notify the user
        Map<String, Object> refundEvent = new HashMap<>();
        refundEvent.put("transactionId", transaction.getId());
        refundEvent.put("senderAccountNumber", transaction.getSenderAccountNumber());
        refundEvent.put("amount", transaction.getAmount());
        refundEvent.put("reason", reason);

        kafkaTemplate.send(TRANSACTION_REFUNDED_TOPIC, transaction.getId(), refundEvent);

        log.info("SAGA compensation complete. Amount: {} refunded to the account: {}",
                transaction.getAmount(), transaction.getSenderAccountNumber());
    }


    private void blockAccountAndCompensateTransaction(Transaction transaction, String reason) {

        //PUBLISH fraud.detected event to Account Service to block the account
        Map<String, Object> refundEvent = new HashMap<>();
        refundEvent.put("transactionId", transaction.getId());
        refundEvent.put("senderAccountNumber", transaction.getSenderAccountNumber());
        refundEvent.put("reason", reason);

        //PUBLISH fraud.detected event to Account Service to block the account
        kafkaTemplate.send(FRAUD_DETECTED_TOPIC, transaction.getSenderAccountNumber(), refundEvent);

        log.warn("Published Fraud detected event - account: {} will be blocked, contact your Bank",
                transaction.getSenderAccountNumber());

        // SAGA Compensation - refund sender
        compensateTransaction(transaction, reason);
    }


    private void completeTransaction(Transaction transaction) {
        transaction.setStatus(TransactionStatus.COMPLETED);
        transaction.setCompletedAt(LocalDateTime.now());
        transactionRepository.save(transaction);

        TransactionCompletedEvent completedEvent = TransactionCompletedEvent.builder()
                .transactionId(transaction.getId())
                .senderAccountNumber(transaction.getSenderAccountNumber())
                .receiverAccountNumber(transaction.getRecipientAccountNumber())
                .amount(transaction.getAmount())
                .description(transaction.getDescription())
                .build();

        //PUBLISH transaction.completed event to Account Service to complete the transaction
        kafkaTemplate.send(TRANSACTION_COMPLETED_TOPIC, transaction.getId(), completedEvent);

        log.info("SAGA COMPLETE - Transaction: {}, completed", transaction.getId());
    }


    public void processFraudCheckCleanResult(String transactionId) {

        Transaction transaction = transactionRepository.findById(transactionId)
                .orElseThrow(() -> new RuntimeException("Transaction not found"));

        if(transaction.getStatus() != TransactionStatus.PROCESSING){
            log.warn("Transaction: {}, is not in PROCESSING status, skipping the transaction.", transactionId);
            return;
        }

        completeTransaction(transaction);
    }


    private TransactionResponse mapToResponse(Transaction savedTransaction) {
        return new TransactionResponse(
                savedTransaction.getId(),
                savedTransaction.getSenderAccountNumber(),
                savedTransaction.getRecipientAccountNumber(),
                savedTransaction.getAmount(),
                savedTransaction.getType(),
                savedTransaction.getStatus(),
                savedTransaction.getDescription(),
                savedTransaction.getFailureReason(),
                savedTransaction.getReferenceNumber(),
                savedTransaction.getCreatedAt(),
                savedTransaction.getCompletedAt()
        );
    }

}
