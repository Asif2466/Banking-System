package com.asif.BankingSystem.account_service.service;

import com.asif.BankingSystem.account_service.dto.AccountCreatedResponseDto;
import com.asif.BankingSystem.account_service.dto.CreateAccountRequestDto;
import com.asif.BankingSystem.account_service.entity.Account;
import com.asif.BankingSystem.account_service.entity.AccountStatus;
import com.asif.BankingSystem.account_service.entity.AccountType;
import com.asif.BankingSystem.account_service.repository.AccountRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.math.BigDecimal;
import java.security.SecureRandom;

@Slf4j
@Service
@RequiredArgsConstructor
public class AccountService {

    private final AccountRepository accountRepository;
    private final SecureRandom secureRandom = new SecureRandom();

    /**
     * create a new bank account
     * @param request
     * @return
     */
    public AccountCreatedResponseDto createAccount(CreateAccountRequestDto request) {
        log.info("Creating account for: {}", request.email());

        if(accountRepository.existsByEmail(request.email())){
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Account already exists");
        }

        Account createdAccount = Account.builder()
                .accountHolderName(request.accountHolderName())
                .email(request.email())
                .phone(request.phone())
                .accountType(request.accountType())
                .accountStatus(AccountStatus.ACTIVE)
                .balance(request.initialDeposit())
                .dailyTransactionLimit(
                        request.accountType() == AccountType.SAVINGS
                        ? new BigDecimal("100000")
                        : new BigDecimal("500000")
                )
                .accountNumber(generateAccountNumber())
                .build();

        Account savedAccount = accountRepository.save(createdAccount);
        log.info("Account created for: {}", request.email());

        return mapToResponse(savedAccount);
    }

    /**
     * Get account by accountNumber
     * @param accountNumber
     * @return
     */
    public AccountCreatedResponseDto getAccount(String accountNumber) {
        Account account = accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(() -> new RuntimeException("Account not found"));

        return mapToResponse(account);
    }

    /**
     * Get account balance
     * @param accountNumber
     * @return
     */
    public BigDecimal getAccountBalance(String accountNumber) {
        Account account = accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(() -> new RuntimeException("Account not found"));

        return account.getBalance();
    }

    /**
     * Block account -- called by Fraud Detection service via Kafka
     * @param accountNumber
     */
    public void blockAccount(String accountNumber){
        log.info("Blocking account: {}", accountNumber);

        Account account = accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(() -> new RuntimeException("Account not found"));

        account.setAccountStatus(AccountStatus.BLOCKED);
        accountRepository.save(account);

        log.info("Account blocked: {}", accountNumber);
    }

    /**
     * Deduct amount from sender account
     * Called by Transaction Service
     * @param accountNumber
     * @param amount
     */
    public void deductAmount(String accountNumber, BigDecimal amount) {
        log.info("Deducting {} from the account: {}", amount, accountNumber);

        Account account = accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(() -> new RuntimeException("Account not found"));

        if(account.getAccountStatus() != AccountStatus.ACTIVE){
            throw new RuntimeException( "Account is not active");
        }

        if(account.getBalance().compareTo(amount) < 0){
            throw new RuntimeException( "Insufficient account balance");
        }

        account.setBalance(account.getBalance().subtract(amount));
        accountRepository.save(account);

        log.info("Amount {} deducted from the account: {}, current Balance: {}",amount, accountNumber, account.getBalance());
    }

    /**
     * credit amount to receiver account
     * Called by Transaction Service
     * @param accountNumber
     * @param amount
     */
    public void creditAmount(String accountNumber, BigDecimal amount) {
        log.info("crediting {} to the account: {}", amount, accountNumber);

        Account account = accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(() -> new RuntimeException("Account not found"));

        if(account.getAccountStatus() != AccountStatus.ACTIVE){
            throw new RuntimeException( "Account is not active");
        }

        account.setBalance(account.getBalance().add(amount));
        accountRepository.save(account);

        log.info("Amount {} credited to account: {}, current Balance: {}", amount, accountNumber, account.getBalance());
    }

    /**
     * Generate unique 12-digit account number
     * @return
     */
    private String generateAccountNumber() {
        String accountNumber;
        do{
            long number = secureRandom.nextLong(1_000_000_000_000L);
            accountNumber = String.format("%012d", number);
        }while(accountRepository.existsByAccountNumber(accountNumber));

        return accountNumber;
    }

    private AccountCreatedResponseDto mapToResponse(Account savedAccount) {
        return new AccountCreatedResponseDto(
                savedAccount.getId(),
                savedAccount.getAccountNumber(), 
                savedAccount.getAccountHolderName(),
                savedAccount.getEmail(),
                savedAccount.getPhone(),
                savedAccount.getAccountType(),
                savedAccount.getAccountStatus(),
                savedAccount.getBalance(),
                savedAccount.getDailyTransactionLimit(),
                savedAccount.getCreatedAt()
        );
    }

}
