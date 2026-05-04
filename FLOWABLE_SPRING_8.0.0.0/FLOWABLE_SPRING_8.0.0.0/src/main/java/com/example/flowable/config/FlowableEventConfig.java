package com.example.flowable.config;

import com.example.flowable.listener.GlobalFlowableListener;
import org.flowable.engine.ProcessEngine;
import org.flowable.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.springframework.context.annotation.Configuration;

import jakarta.annotation.PostConstruct;

@Configuration
public class FlowableEventConfig {

    private final ProcessEngine processEngine;
    private final GlobalFlowableListener listener;

    public FlowableEventConfig(ProcessEngine processEngine,
                               GlobalFlowableListener listener) {
        this.processEngine = processEngine;
        this.listener = listener;
        System.out.println("🔥 CONFIG CLASS LOADED");
    }

    @PostConstruct
    public void init() {

        ProcessEngineConfigurationImpl config =
            (ProcessEngineConfigurationImpl) processEngine.getProcessEngineConfiguration();

        config.getEventDispatcher().addEventListener(listener);

        System.out.println("🔥 FLOWABLE LISTENER REGISTERED");
    }
}