package com.asif.BankingSystem.account_service.dto;

import com.asif.BankingSystem.account_service.entity.AccountType;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;

public record CreateAccountRequestDto (

        @NotNull(message = "Account holder name is required")
        String accountHolderName,

        @NotNull(message = "Email is required")
        @Email(message = "Invalid mail format")
        String email,

        @NotNull(message = "Phone number is required")
        String phone,

        @NotNull(message = "Account type is required")
        AccountType accountType,

        @NotNull(message = "Initial deposit is required")
        @Positive(message = "Initial deposit should be positive")
        BigDecimal initialDeposit
){
}
