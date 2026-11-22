package com.asif.BankingSystem.transaction_service.dto;

import com.asif.BankingSystem.transaction_service.entity.TransactionStatus;
import com.asif.BankingSystem.transaction_service.entity.TransactionType;
import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public record TransactionResponse(
        String id,
        String senderAccountNumber,
        String recipientAccountNumber,
        BigDecimal amount,
        TransactionType type,
        TransactionStatus status,
        String description,
        String failureReason,
        String referenceNumber,
        LocalDateTime createdAt,
        LocalDateTime completedAt
){
    }
