package com.example.flowable.service;

import org.flowable.engine.RepositoryService;
import org.flowable.engine.RuntimeService;
import org.flowable.engine.TaskService;
import org.flowable.engine.repository.Deployment;
import org.flowable.engine.repository.ProcessDefinition;
import org.flowable.engine.runtime.ProcessInstance;
import org.flowable.task.api.Task;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class FlowableService {

    private final RepositoryService repositoryService;
    private final RuntimeService runtimeService;
    private final TaskService taskService;

    public FlowableService(RepositoryService repositoryService,
                           RuntimeService runtimeService,
                           TaskService taskService) {
        this.repositoryService = repositoryService;
        this.runtimeService = runtimeService;
        this.taskService = taskService;
    }

    // =========================================================
    // 1. DEPLOY PROCESS DEFINITION
    // =========================================================
    public String deployProcess(String bpmnFilePath) {

        Deployment deployment = repositoryService.createDeployment()
                .addClasspathResource(bpmnFilePath)
                .name("Dynamic Deployment")
                .deploy();

        return deployment.getId();
    }

    // =========================================================
    // 2. LIST DEPLOYED PROCESS DEFINITIONS
    // =========================================================
    public List<ProcessDefinition> getDeployedProcesses() {

        return repositoryService.createProcessDefinitionQuery()
                .latestVersion()
                .list();
    }

    // =========================================================
    // 3. START PROCESS
    // =========================================================
    public String startProcess(String processKey,
                               Map<String, Object> variables) {

        ProcessInstance instance =
                runtimeService.startProcessInstanceByKey(processKey, variables);

        return instance.getId();
    }

    // Overload (no variables)
    public String startProcess(String processKey) {
        return startProcess(processKey, new HashMap<>());
    }

    // =========================================================
    // 4. GET PENDING USER TASKS
    // =========================================================
    public List<Task> getPendingTasks(String assignee) {

        return taskService.createTaskQuery()
                .taskAssignee(assignee)
                .active()
                .list();
    }

    // =========================================================
    // 5. CLAIM USER TASK
    // =========================================================
    public void claimTask(String taskId, String userId) {

        taskService.claim(taskId, userId);
    }

    // =========================================================
    // 6. COMPLETE USER TASK
    // =========================================================
    public void completeTask(String taskId) {

        taskService.complete(taskId);
    }

    // Overload with variables
    public void completeTask(String taskId, Map<String, Object> variables) {

        taskService.complete(taskId, variables);
    }
}