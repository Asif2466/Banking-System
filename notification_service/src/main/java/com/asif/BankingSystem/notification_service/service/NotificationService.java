package com.asif.BankingSystem.notification_service.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class NotificationService {

    @KafkaListener(topics = "transaction.otp.generated")
    public void consumeOtpGeneratedEvent(@Payload Map<String, Object> payload) {
        try{
            String transactionId = (String) payload.get("transactionId");
            String accountNumber = (String) payload.get("accountNumber");
            String otp = (String) payload.get("otp");
            String amount = (String) payload.get("amount");
            String reason = (String) payload.get("reason");

            sendAlert(accountNumber,
                    "TRANSACTION VERIFICATION REQUIRED",
                    String.format(
                            "Suspicious activity detected on your account %s."+
                            "Reason: %s"+
                            "A transaction of %s is pending for verification."+
                            "Your OTP is %s. It will be only valid for 5 minutes."+
                            "If this wasn't you, ignore this message.",
                            accountNumber, reason, amount, otp
                    )
            );
        } catch (Exception e) {
            log.error("Error while sending OTP notification: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "transaction.completed")
    public void consumeTransactionCompletedEvent(@Payload Map<String, Object> payload) {
        try{
            String senderAccountNumber = payload.get("senderAccountNumber").toString();
            String receiverAccountNumber = payload.get("receiverAccountNumber").toString();
            String amount = (String) payload.get("amount");

            // DEBIT ALERT
            sendAlert(senderAccountNumber,
                    "DEBIT ALERT",
                    String.format(
                            "%s debited from your account @s",
                            amount, senderAccountNumber
                    )
            );

            // CREDIT ALERT
            sendAlert(receiverAccountNumber,
                    "CREDIT ALERT",
                    String.format(
                            "%s credited to your account @s",
                            amount, receiverAccountNumber
                    )
            );

        } catch (Exception ex){
            log.error("Error while credit/debit alerting: {}", ex.getMessage());
        }
    }

    @KafkaListener(topics = "fraud.detected")
    public void consumeFraudDetectedEvent(@Payload Map<String, Object> payload) {
        try{
            String senderAccountNumber = payload.get("senderAccountNumber").toString();
            String reason = (String) payload.get("reason");

            // FRAUD DETECTED ALERT
            sendAlert(senderAccountNumber,
                    "SUSPICIOUS ACTIVITY DETECTED",
                    String.format(
                            "Your account %s has been blocked. "+
                            "Reason: %s. "+
                            "Please contact your Bank immediately.",
                            senderAccountNumber, reason
                    )
            );
        } catch (Exception ex) {
            log.error("Error while fraud-detected alerting: {}", ex.getMessage());
        }
    }

    @KafkaListener(topics = "transaction.refunded")
    public void consumeTransactionRefundedEvent(@Payload Map<String, Object> payload) {
        try{
            String senderAccountNumber = payload.get("senderAccountNumber").toString();
            String amount = payload.get("amount").toString();
            String reason = (String) payload.get("reason");

            // FRAUD DETECTED ALERT
            sendAlert(senderAccountNumber,
                    "REFUND PROCESSED",
                    String.format(
                            "Your transaction for the amount %s has been cancelled. "+
                            "Reason: %s ."+
                            "Amount %s has been refunded to your account %s. ",
                            amount, reason, amount, senderAccountNumber
                    )
            );
        } catch (Exception ex) {
            log.error("Error while Transaction refund alerting: {}", ex.getMessage());
        }
    }

    @KafkaListener(topics = "payment.completed")
    public void consumePaymentCompletedEvent(@Payload Map<String, Object> payload) {
        try{
            String accountNumber = payload.get("accountNumber").toString();
            String amount = payload.get("amount").toString();
            String razorpayPaymentId = (String) payload.get("razorpayPaymentId");

            // PAYMENT COMPLETED ALERT
            sendAlert(accountNumber,
                    "PAYMENT COMPLETED",
                    String.format(
                            "Payment of %s is Completed. "+
                            "Razorpay Id: %s. ",
                            amount, razorpayPaymentId
                    )
            );
        } catch (Exception ex) {
            log.error("Error while Payment completed alerting: {}", ex.getMessage());
        }
    }

    @KafkaListener(topics = "payment.failed")
    public void consumePaymentFailedEvent(@Payload Map<String, Object> payload) {
        try{
            String accountNumber = payload.get("accountNumber").toString();
            String amount = payload.get("amount").toString();
            String reason = (String) payload.get("reason");

            // PAYMENT FAILED ALERT
            sendAlert(accountNumber,
                    "PAYMENT FAILED",
                    String.format(
                            "Your payment for the amount %s could not be processed. "+
                            "Please try again or contact support. "+
                            "Reason: %s .",
                            amount, reason
                    )
            );
        } catch (Exception ex) {
            log.error("Error while Payment failed alerting: {}", ex.getMessage());
        }
    }

    private void sendAlert(String accountNumber, String subject, String message) {

        log.info("-----------------------------------------");
        log.info("Subject: {}", subject);
        log.info("Account Number: {}", accountNumber);
        log.info("Message: {}", message);
        log.info("-----------------------------------------");

    }
}
