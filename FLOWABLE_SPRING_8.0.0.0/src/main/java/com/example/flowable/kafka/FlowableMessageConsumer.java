package com.example.flowable.kafka;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import java.util.HashMap;
import java.util.Map;

@Component
public class FlowableMessageConsumer {

 private final RestTemplate restTemplate = new RestTemplate();

    // Flowable REST base URL
    private final String flowableUrl = "http://localhost:8080/process-api/";
	
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
	
	 private void resumeFlowableProcess(String executionId) {

        String url = flowableUrl + "/runtime/executions/" + executionId;

        Map<String, Object> request = new HashMap<>();

        // This triggers message event internally
        request.put("action", "signalEventReceived");
        request.put("signalName", "loanApprovedMessage");

        restTemplate.postForObject(url, request, String.class);

        System.out.println("Flowable resumed via REST for: " + executionId);
    }
}
