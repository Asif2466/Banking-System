package com.asif.BankingSystem.transaction_service.controller;

import com.asif.BankingSystem.transaction_service.dto.TransactionResponse;
import com.asif.BankingSystem.transaction_service.dto.TransferRequest;
import com.asif.BankingSystem.transaction_service.service.TransactionService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Slf4j
@RestController
@RequestMapping("/api/v1/transactions")
@RequiredArgsConstructor
public class TransactionController {

    private final TransactionService transactionService;

    @PostMapping("/transfer")
    public ResponseEntity<TransactionResponse> transfer(
            @Valid @RequestBody TransferRequest request
            ){
        return new ResponseEntity<>(transactionService.transfer(request), HttpStatus.CREATED);
    }

    @GetMapping("/{transactionId}")
    public ResponseEntity<TransactionResponse> getTransaction(
            @PathVariable String transactionId
    ){
        return ResponseEntity.ok(transactionService.getTransaction(transactionId));
    }

    @GetMapping("/transactionHistory/{accountNumber}")
    public ResponseEntity<List<TransactionResponse>> getTransactionHistory(
            @PathVariable String accountNumber
    ){
        return ResponseEntity.ok(transactionService.getTransactionHistory(accountNumber));
    }

    @PostMapping("/{transactionId}/verifyOtp")
    public ResponseEntity<TransactionResponse> verifyOTP(
        @PathVariable String transactionId,
        @RequestParam String otp
    ){
        log.info("Verify OTP for transaction id {}", transactionId);
        return ResponseEntity.ok(transactionService.verifyOTP(transactionId, otp));
    }
}
