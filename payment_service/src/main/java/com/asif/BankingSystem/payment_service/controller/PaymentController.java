package com.asif.BankingSystem.payment_service.controller;

import com.asif.BankingSystem.payment_service.dto.CreatePaymentRequest;
import com.asif.BankingSystem.payment_service.dto.PaymentOrderResponse;
import com.asif.BankingSystem.payment_service.service.PaymentService;
import com.razorpay.RazorpayException;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/v1/payments")
@RequiredArgsConstructor
public class PaymentController {

    private final PaymentService paymentService;

    @PostMapping("/create-order")
    public ResponseEntity<PaymentOrderResponse> createPaymentOrder(
            @Valid @RequestBody CreatePaymentRequest request) throws RazorpayException {
        return ResponseEntity.status(HttpStatus.CREATED).body(paymentService.createPaymentOrder(request));
    }

    //Razorpay webhook endpoint
    // Webhooks -> it captures events, contains 100s of events like payment success, failure etc.,
    @PostMapping("/webhook")
    public ResponseEntity<String> handleWebhook( @RequestBody Map<String, Object> payload){
        paymentService.handleWebhook(payload);
        return ResponseEntity.ok("Webhook Processed");
    }
}
