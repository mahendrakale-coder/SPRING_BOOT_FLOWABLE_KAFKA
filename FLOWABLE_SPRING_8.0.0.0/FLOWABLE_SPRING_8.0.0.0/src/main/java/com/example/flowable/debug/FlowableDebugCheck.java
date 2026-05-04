package com.example.flowable.debug;

import org.flowable.engine.ProcessEngine;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;

@Component
public class FlowableDebugCheck {

	@PostConstruct
	public void init() {
		System.out.println("🔥 FlowableEventConfig INITIALIZED");
	}

    private final ProcessEngine processEngine;

    public FlowableDebugCheck(ProcessEngine processEngine) {
        this.processEngine = processEngine;
    }

    @PostConstruct   
	public void check() {

    var dispatcher =
        processEngine.getProcessEngineConfiguration().getEventDispatcher();

    System.out.println("EVENT DISPATCHER = " + dispatcher);

    if (dispatcher == null) {
        System.out.println("❌ EVENT SYSTEM IS DISABLED");
    } else {
        System.out.println("✅ EVENT SYSTEM IS ACTIVE");
    }
}
}