package com.example.flowable.kafka;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class FlowableMessageConsumer {

    /**
     * This method reads messages from the topic.
     * Flowable 8 Outbound Channels typically send JSON strings.
     */
    @KafkaListener(topics = "loan-events", groupId = "flowable-group")
    public void consume(String message) {
        try {
            // Process the message (e.g., save to your own DB, log it, etc.)
            System.out.println("Received Message from Flowable Topic: " + message);
            
            // If you need to parse the JSON:
            // MyObject obj = objectMapper.readValue(message, MyObject.class);
            
        } catch (Exception e) {
            System.err.println("Error processing Kafka message: " + e.getMessage());
        }
    }
}
