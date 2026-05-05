package com.example.flowable.listener;

import org.flowable.engine.ProcessEngine;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
//import com.example.flowable.kafka.KafkaPublisher;
import com.example.flowable.kafka.EventQueueService;
import org.springframework.beans.factory.annotation.Autowired;

@Component
public class FlowableStartupListener {

    private final ProcessEngine processEngine;
    //private final KafkaPublisher kafkaPublisher;
	
	@Autowired
	private EventQueueService queueService;

    /*public FlowableStartupListener(ProcessEngine processEngine,
                                   KafkaPublisher kafkaPublisher) {
        this.processEngine = processEngine;
        this.kafkaPublisher = kafkaPublisher;
    }*/

	public FlowableStartupListener(ProcessEngine processEngine) {
        this.processEngine = processEngine;
    }
    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {

        System.out.println("🚀 Spring Boot Application Started");

        // ✔ Correct check (ProcessEngine is always injected if app starts)
        if (processEngine == null) {
            throw new IllegalStateException("❌ Flowable Engine not available");
        }

        System.out.println("🔥 Flowable Engine is READY");
		
		
        initializeApplicationWorkflows();
		
		String engineName = processEngine.getName();
		
        System.out.println("Engine Name: " + engineName);

        // OPTIONAL: safer way to validate DB connection indirectly
		long processCount = 0;
        try 
		{
            processCount = processEngine
                    .getRepositoryService()
                    .createProcessDefinitionQuery()
                    .count();

            System.out.println("📦 Deployed Process Definitions: " + processCount);

        } 
		catch (Exception e) 
		{
            throw new IllegalStateException("❌ Flowable Engine initialization failed", e);
        }
		
		
        String eventJson = "{\"eventType\": \"FLOWABLE_ENGINE_STARTED\",\"engineName\": \"%s\",\"processDefinitions\": %d,\"status\": \"READY\"}".formatted(engineName, processCount);

        //kafkaPublisher.publish("loan-events", eventJson);
		queueService.publish(eventJson);
    }

    private void initializeApplicationWorkflows() {

        System.out.println("⚙️ Running Flowable startup initialization...");

        // Example safe startup operations:

        // 👉 Examples you can add here:

        // 1. Auto-deploy BPMN processes
        // deployProcess("processes/loanApproval.bpmn20.xml");

        // 2. Warm up cache / preload definitions
        // processEngine.getRepositoryService().createProcessDefinitionQuery().list();

        // 3. Trigger startup process if needed
        // processEngine.getRuntimeService()
        //        .startProcessInstanceByKey("startupProcess");
		
        //processEngine.getRepositoryService()
        //        .createProcessDefinitionQuery()
        //        .list();

        // 2. Optional: trigger startup workflow
        // processEngine.getRuntimeService()
        //        .startProcessInstanceByKey("startupProcess");

        System.out.println("✅ Flowable startup initialization completed");
    }
}