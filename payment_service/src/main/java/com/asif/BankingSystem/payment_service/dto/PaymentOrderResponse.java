package com.asif.BankingSystem.payment_service.dto;

import java.math.BigDecimal;

public record PaymentOrderResponse(
        String paymentId,
        String razorPayOrderId,
        BigDecimal amount,
        String currency,
        String status,
        String razorPayKeyId
) {
}
