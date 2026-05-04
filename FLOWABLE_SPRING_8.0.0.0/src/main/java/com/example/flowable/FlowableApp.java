package com.example.flowable;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "com.example.flowable")
public class FlowableApp {
    public static void main(String[] args) {
        SpringApplication.run(FlowableApp.class, args);
    }
}