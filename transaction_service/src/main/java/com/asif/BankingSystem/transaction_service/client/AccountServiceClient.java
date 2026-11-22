package com.asif.BankingSystem.transaction_service.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.math.BigDecimal;

@FeignClient(name = "account-service", url = "${account.service.url}")
public interface AccountServiceClient {

    @PutMapping("/{accountNumber}/credit")
    public String creditAmount(
            @PathVariable String accountNumber,
            @RequestParam BigDecimal amount);

    @PutMapping("/{accountNumber}/deduct")
    public String deductAmount(
            @PathVariable String accountNumber,
            @RequestParam BigDecimal amount);

}
