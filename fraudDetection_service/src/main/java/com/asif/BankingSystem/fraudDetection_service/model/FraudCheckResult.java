package com.asif.BankingSystem.fraudDetection_service.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class FraudCheckResult {

    private boolean fraud;
    private String reason;

}
