package com.example.flowable.delegate;

import org.flowable.engine.delegate.DelegateExecution;
import org.flowable.engine.delegate.JavaDelegate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component("kafkaSendDelegate")
public class KafkaSendDelegate implements JavaDelegate {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaSendDelegate(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void execute(DelegateExecution execution) {

        String message = "Document approved: " + execution.getProcessInstanceId();

        kafkaTemplate.send("loan-events", message);

        System.out.println("Kafka message sent: " + message);
    }
}