package com.asif.BankingSystem.payment_service.service;

import com.asif.BankingSystem.payment_service.dto.CreatePaymentRequest;
import com.asif.BankingSystem.payment_service.dto.PaymentOrderResponse;
import com.asif.BankingSystem.payment_service.entity.Payment;
import com.asif.BankingSystem.payment_service.entity.PaymentStatus;
import com.asif.BankingSystem.payment_service.repository.PaymentRepository;
import com.razorpay.Order;
import com.razorpay.RazorpayClient;
import com.razorpay.RazorpayException;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentService {

    private final PaymentRepository paymentRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${razorpay.key-id}")
    private String keyId;

    @Value("${razorpay.secret-key}")
    private String secretKey;

    private static final String PAYMENT_COMPLETED_TOPIC = "payment.completed";
    private static final String PAYMENT_FAILED_TOPIC = "payment.failed";

    /**
     * Create Razorpay Order
     *
     * FLOW:
     * 1. Create order in RazorPay(payment-gateway)
     * 2. Save payment record in DB
     * 3. Return order details to front-end
     * 4. Front-end show RazorPay checkout page
     * 5. User pays
     * 6. RazorPay calls webhook
     * @param request
     * @return
     */
    public PaymentOrderResponse createPaymentOrder(@Valid CreatePaymentRequest request) throws RazorpayException {
        log.info("creating payment order for the account: {}, with the amount: {}",
                request.accountNumber(), request.amount());

        RazorpayClient razorpayClient = new RazorpayClient(keyId, secretKey);

        // convert amount USD/RUPEE to cents to paise
        int convertedAmount = request.amount().multiply(BigDecimal.valueOf(100)).intValue();

        JSONObject orderRequest = new JSONObject();
        orderRequest.put("amount", convertedAmount);
        orderRequest.put("currency", "USD/INR");// should use anyone USD or Rupee
        orderRequest.put("receipt", "rcpt_"+System.currentTimeMillis()+ UUID.randomUUID().toString()
                                            .replace("-", "").substring(0,10));

        //creating Razorpay order
        Order razorpayOrder = razorpayClient.orders.create(orderRequest);

        log.info("Razorpay order created: {}", razorpayOrder.get("id").toString());

        // save the payment record
        Payment payment = Payment.builder()
                .razorPayOrderId(razorpayOrder.get("id").toString())
                .accountNumber(request.accountNumber())
                .amount(request.amount())
                .currency("USD/INR")// should use anyone USD or Rupee
                .status(PaymentStatus.CREATED)
                .description(request.description())
                .build();
        Payment savedpayment = paymentRepository.save(payment);

        return new PaymentOrderResponse(
                savedpayment.getId(),
                razorpayOrder.get("id").toString(),
                request.amount(),
                "USD/INR",
                "CREATED",
                keyId);

    }


    public void handleWebhook(Map<String, Object> payload) {
        log.info("Received Razorpay webhook: {}", payload.get("event"));

        String event = payload.get("event").toString();

        if(event.equals("payment.captured")){
            handlePaymentSuccess(payload);
        }
        else if(event.equals("payment.failed")){
            handlePaymentFailure(payload);
        }
    }

    private void handlePaymentSuccess(Map<String, Object> payload) {
        try {
            Map<String, Object> paymentData = extractPaymentData(payload);

            String orderId = paymentData.get("order_id").toString();
            String razorpayPaymentId = paymentData.get("id").toString();

            Payment payment = paymentRepository.findByRazorPayOrderId(orderId)
                    .orElseThrow(() -> new RuntimeException("Payment not found"));

            payment.setRazorPayOrderId(razorpayPaymentId);
            payment.setStatus(PaymentStatus.COMPLETED);
            paymentRepository.save(payment);

            // publish event to Kafka -- Notification service
            Map<String, Object> paymentEvent = new HashMap<>();
            paymentEvent.put("paymentId", payment.getId());
            paymentEvent.put("accountNumber", payment.getAccountNumber());
            paymentEvent.put("amount", payment.getAmount());
            paymentEvent.put("razorpayPaymentId", payment.getRazorPayPaymentId());

            kafkaTemplate.send(PAYMENT_COMPLETED_TOPIC, payment.getId(), paymentEvent);
            log.info("Payment completed: {}", payment.getId());

        } catch (Exception e) {
            log.error("Error occurred while handling payment success: {}", e.getMessage());
        }
    }

    private void handlePaymentFailure(Map<String, Object> payload) {
        try{
            Map<String, Object> paymentData = extractPaymentData(payload);

            String orderId = paymentData.get("order_id").toString();
            Payment payment = paymentRepository.findByRazorPayOrderId(orderId)
                    .orElseThrow(() -> new RuntimeException("Payment not found"));

            payment.setStatus(PaymentStatus.FAILED);
            payment.setFailureReason("Payment failed via Razorpay");
            paymentRepository.save(payment);

            // publish event to Kafka -- Notification service
            Map<String, Object> paymentEvent = new HashMap<>();
            paymentEvent.put("paymentId", payment.getId());
            paymentEvent.put("accountNumber", payment.getAccountNumber());
            paymentEvent.put("amount", payment.getAmount());
            paymentEvent.put("reason", payment.getFailureReason());

            kafkaTemplate.send(PAYMENT_FAILED_TOPIC, payment.getId(), paymentEvent);
            log.warn("Payment failed: {}", payment.getId());

        } catch (Exception e) {
            log.error("Error occurred while handling payment failure: {}", e.getMessage());
        }
    }

    private Map<String, Object> extractPaymentData(Map<String, Object> payload) {
        Map<String, Object> entity = (Map<String, Object>) payload.get("payload");

        Map<String, Object> paymentWrapper = (Map<String, Object>) entity.get("payment");

        return (Map<String, Object>) paymentWrapper.get("entity");
    }
}
