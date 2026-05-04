package com.example.flowable.client;

import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

@Service
public class FlowableRestClient {

    private final WebClient webClient;

    public FlowableRestClient() {
        this.webClient = WebClient.builder()
                .baseUrl("http://localhost:8080/process-api/")
                .defaultHeaders(h -> h.setBasicAuth("admin", "test"))
                .build();
    }

    // =========================================================
    // 1. DEPLOYED PROCESSES
    // =========================================================
    public String getDeployedProcesses() {
        return webClient.get()
                .uri("/repository/process-definitions")
                .retrieve()
                .bodyToMono(String.class)
                .block();
    }

    // =========================================================
    // 2. START PROCESS
    // =========================================================
    public String startProcess(String processKey) {
        return webClient.post()
                .uri("/repository/process-definitions")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("""
                        {
                          "processDefinitionKey": "%s"
                        }
                        """.formatted(processKey))
                .retrieve()
                .bodyToMono(String.class)
                .block();
    }

    // =========================================================
    // 3. GET PENDING TASKS (by assignee)
    // =========================================================
    public String getPendingTasks(String assignee) {
        return webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/runtime/tasks")
                        .queryParam("assignee", assignee)
                        .build())
                .retrieve()
                .bodyToMono(String.class)
                .block();
    }

    // =========================================================
    // 4. CLAIM TASK
    // =========================================================
    public String claimTask(String taskId, String userId) {
        return webClient.post()
                .uri("/runtime/tasks/{id}", taskId)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("""
                        {
                          "action": "claim",
                          "assignee": "%s"
                        }
                        """.formatted(userId))
                .retrieve()
                .bodyToMono(String.class)
                .block();
    }

    // =========================================================
    // 5. COMPLETE TASK
    // =========================================================
    public String completeTask(String taskId) {
        return webClient.post()
                .uri("/runtime/tasks/{id}", taskId)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("""
                        {
                          "action": "complete"
                        }
                        """)
                .retrieve()
                .bodyToMono(String.class)
                .block();
    }
}