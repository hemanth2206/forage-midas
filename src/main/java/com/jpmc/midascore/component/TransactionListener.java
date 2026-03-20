package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class TransactionListener {
    private final UserRepository userRepository;
    private final TransactionRepository transactionRepository;
    private final IncentiveApiClient incentiveApiClient;

    public TransactionListener(UserRepository userRepository,
                               TransactionRepository transactionRepository,
                               IncentiveApiClient incentiveApiClient) {
        this.userRepository = userRepository;
        this.transactionRepository = transactionRepository;
        this.incentiveApiClient = incentiveApiClient;
    }

    @Transactional
    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core")
    public void onTransaction(Transaction transaction) {
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());

        if (sender == null || recipient == null) {
            return;
        }

        float amount = transaction.getAmount();
        if (sender.getBalance() < amount) {
            return;
        }

        float incentive = incentiveApiClient.fetchIncentiveAmount(transaction);

        sender.setBalance(sender.getBalance() - amount);
        recipient.setBalance(recipient.getBalance() + amount + incentive);

        userRepository.save(sender);
        userRepository.save(recipient);
        transactionRepository.save(new TransactionRecord(sender, recipient, amount, incentive));
    }
}
