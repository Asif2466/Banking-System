package com.asif.BankingSystem.fraudDetection_service.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

import java.math.BigDecimal;

@FeignClient(name = "account-service", url = "${account.service.url}")
public interface AccountServiceClient {

    @GetMapping("/{accountNumber}/balance")
    public BigDecimal getAccountBalance(
            @PathVariable String accountNumber);
}
