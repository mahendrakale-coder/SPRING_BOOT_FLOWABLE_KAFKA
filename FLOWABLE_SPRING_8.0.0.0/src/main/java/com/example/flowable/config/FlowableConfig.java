package com.example.flowable.config;
import org.flowable.spring.boot.EngineConfigurationConfigurer;

import org.springframework.context.annotation.Configuration;
import org.flowable.engine.impl.cfg.ProcessEngineConfigurationImpl;

import com.example.flowable.listener.GlobalFlowableListener;

@Configuration
public class FlowableConfig {

    private final GlobalFlowableListener listener;

    public FlowableConfig(GlobalFlowableListener listener) {
        this.listener = listener;
    }

    public EngineConfigurationConfigurer<ProcessEngineConfigurationImpl> configurer() {
        return engineConfiguration -> {
            engineConfiguration.getEventDispatcher()
                    .addEventListener(listener);
        };
    }
}

