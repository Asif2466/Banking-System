package com.asif.BankingSystem.account_service.dto;

import com.asif.BankingSystem.account_service.entity.AccountStatus;
import com.asif.BankingSystem.account_service.entity.AccountType;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public record AccountCreatedResponseDto(

        String id,
        String accountNumber,
        String accountHolderName,
        String email,
        String phone,
        AccountType accountType,
        AccountStatus accountStatus,
        BigDecimal balance,
        BigDecimal dailyTransactionLimit,
        LocalDateTime createdAt

) {
}
