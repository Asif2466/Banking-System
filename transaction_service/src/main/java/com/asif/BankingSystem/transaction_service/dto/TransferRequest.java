package com.asif.BankingSystem.transaction_service.dto;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;


public record TransferRequest(

        @NotNull(message = "Sender account number is required")
        String senderAccountNumber,

        @NotNull(message = "Receiver account number is required")
        String recipientAccountNumber,

        @NotNull(message = "Amount is required")
        @Positive(message = "Amount should be positive")
        BigDecimal amount,

        String description
) {
}
