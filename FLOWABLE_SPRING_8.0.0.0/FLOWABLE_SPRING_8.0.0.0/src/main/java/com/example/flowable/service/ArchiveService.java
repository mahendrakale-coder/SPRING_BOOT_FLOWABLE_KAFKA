package com.example.flowable.service;

import org.flowable.engine.delegate.DelegateExecution;
import org.flowable.engine.delegate.JavaDelegate;
import org.springframework.stereotype.Service;

@Service("archiveService")
public class ArchiveService implements JavaDelegate {

    @Override
    public void execute(DelegateExecution execution) {
        System.out.println("Archiving process: " +
            execution.getProcessInstanceId());
    }
}