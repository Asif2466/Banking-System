package com.asif.BankingSystem.account_service.controller;

import com.asif.BankingSystem.account_service.dto.AccountCreatedResponseDto;
import com.asif.BankingSystem.account_service.dto.CreateAccountRequestDto;
import com.asif.BankingSystem.account_service.service.AccountService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;

@Slf4j
@RestController
@RequestMapping("/api/v1/accounts")
@RequiredArgsConstructor
public class AccountController {

    private final AccountService accountService;

    @PostMapping
    public ResponseEntity<AccountCreatedResponseDto> createAccount(
            @Valid @RequestBody CreateAccountRequestDto request) {
        return new ResponseEntity<>(accountService.createAccount(request), HttpStatus.CREATED);
    }

    @GetMapping("/{accountNumber}")
    public ResponseEntity<AccountCreatedResponseDto> getAccount(
            @PathVariable String accountNumber) {
        return ResponseEntity.ok(accountService.getAccount(accountNumber));
    }

    @GetMapping("/{accountNumber}/balance")
    public ResponseEntity<BigDecimal> getAccountBalance(
            @PathVariable String accountNumber) {
        return ResponseEntity.ok(accountService.getAccountBalance(accountNumber));
    }

    @PutMapping("/{accountNumber}/block")
    public ResponseEntity<String> blockAccount(
            @PathVariable String accountNumber){
        accountService.blockAccount(accountNumber);
        return ResponseEntity.ok("Account blocked successfully");
    }

    /**
     * SAGA step 1 - Deduct amount
     * called Transaction service when transfer is initiated
    * */
    @PutMapping("/{accountNumber}/deduct")
    public ResponseEntity<String> deductAmount(
            @PathVariable String accountNumber,
            @RequestParam BigDecimal amount){
        accountService.deductAmount(accountNumber, amount);
        return ResponseEntity.ok("Amount deducted successfully");
    }

    /**
     * SAGA step 4 - Compensating transaction endpoint
     *  called by Transaction service in TWO SCENARIOS
     *  1. Fraud deducted - refund sender (UNDO step 1)
     *  2. Transaction completed - credit receiver.
     */
    @PutMapping("/{accountNumber}/credit")
    public ResponseEntity<String> creditAmount(
            @PathVariable String accountNumber,
            @RequestParam BigDecimal amount){
        accountService.creditAmount(accountNumber, amount);
        return ResponseEntity.ok("Amount deducted successfully");
    }

}
