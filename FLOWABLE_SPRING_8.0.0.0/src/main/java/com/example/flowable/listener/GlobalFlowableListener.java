package com.example.flowable.listener;

import com.example.flowable.kafka.EventQueueService;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.flowable.common.engine.api.delegate.event.*;
import org.flowable.engine.RuntimeService;
import org.flowable.engine.runtime.Execution;
import org.flowable.engine.runtime.ProcessInstance;
import org.flowable.task.api.Task;
import org.flowable.engine.delegate.event.FlowableActivityEvent;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class GlobalFlowableListener implements FlowableEventListener {

    @Autowired
    private RuntimeService runtimeService;

    @Autowired
    private EventQueueService queueService;

    // ✅ reuse (VERY IMPORTANT for performance)
    private static final ObjectMapper objectMapper = new ObjectMapper();

	private Map<String, Object> payload = new HashMap<>(16);

    @Override
    public void onEvent(FlowableEvent event) {

        try {
            FlowableEngineEventType type = (FlowableEngineEventType) event.getType();

			System.out.println("GlobalFlowableListener.java : " + type);
            // ❌ ignore noisy low-value events
            if (type == FlowableEngineEventType.ENTITY_CREATED ||
                type == FlowableEngineEventType.ENTITY_UPDATED ||
                type == FlowableEngineEventType.ENTITY_DELETED ||
                type == FlowableEngineEventType.ENTITY_INITIALIZED ||
                type == FlowableEngineEventType.ENTITY_SUSPENDED ||
                type == FlowableEngineEventType.ENTITY_ACTIVATED) {
                return;
            }

            
			
            payload.put("eventType", type.name());
            payload.put("timestamp", System.currentTimeMillis());

			if (event instanceof  org.flowable.engine.delegate.event.FlowableSequenceFlowTakenEvent activityEvent) 
			{
                if (type == FlowableEngineEventType.SEQUENCEFLOW_TAKEN )
				{					
					payload.put("id", activityEvent.getId());
					payload.put("sourceActivityBehaviorClass",activityEvent.getSourceActivityBehaviorClass());
					payload.put("sourceActivityId",activityEvent.getSourceActivityId());
					payload.put("sourceActivityName",activityEvent.getSourceActivityName());
					payload.put("sourceActivityType",activityEvent.getSourceActivityType());
					payload.put("targetActivityBehaviorClass",activityEvent.getTargetActivityBehaviorClass());
					payload.put("targetActivityId",activityEvent.getTargetActivityId());
					payload.put("targetActivityName",activityEvent.getTargetActivityName());
					payload.put("targetActivityType" ,activityEvent.getTargetActivityType()); 
					payload.put("executionId",activityEvent.getExecutionId());
					payload.put("processDefinitionId",activityEvent.getProcessDefinitionId());
					payload.put("processInstanceId",activityEvent.getProcessInstanceId());
					payload.put("scopeDefinitionId",activityEvent.getScopeDefinitionId());
					payload.put("scopeId",activityEvent.getScopeId());
					payload.put("scopeType",activityEvent.getScopeType());
					payload.put("subScopeId",activityEvent.getSubScopeId());					
					
					enqueue(payload);
				   return ;					
				}
			}
			
            // =========================
            // 🔹 SERVICE TASK (ACTIVITY)
            // =========================
            if (event instanceof FlowableActivityEvent activityEvent) {

                String activityType = activityEvent.getActivityType();

                if ("serviceTask".equals(activityType) &&
                    (type == FlowableEngineEventType.ACTIVITY_STARTED ||
                     type == FlowableEngineEventType.ACTIVITY_COMPLETED)) {

                    payload.put("eventCategory", "SERVICE_TASK");
                    payload.put("status",type == FlowableEngineEventType.ACTIVITY_STARTED ? "STARTED" : "COMPLETED");

                    payload.put("processInstanceId", activityEvent.getProcessInstanceId());
                    payload.put("executionId", activityEvent.getExecutionId());
                    payload.put("activityId", activityEvent.getActivityId());
                    payload.put("activityName", activityEvent.getActivityName());
                    payload.put("activityType", activityType);

                   enqueue(payload);
				   return ;
                }
            }

            // =========================
            // 🔹 SERVICE TASK ERRORS (ASYNC JOB)
            // =========================
            else  if (type == FlowableEngineEventType.JOB_CANCELED  ||
						type == FlowableEngineEventType.JOB_EXECUTION_FAILURE  ||
						type == FlowableEngineEventType.JOB_EXECUTION_SUCCESS  ||
						type == FlowableEngineEventType.JOB_MOVED_TO_DEADLETTER  ||
						type == FlowableEngineEventType.JOB_REJECTED ||
						type == FlowableEngineEventType.JOB_RESCHEDULED  ||
						type == FlowableEngineEventType.JOB_RETRIES_DECREMENTED ) 
					{
					 
                    payload.put("eventCategory", "SERVICE_TASK_JOB");


                    payload.put("status",type == FlowableEngineEventType.JOB_EXECUTION_SUCCESS ? "COMPLETED" : "FAILED");

                    if (type == FlowableEngineEventType.JOB_EXECUTION_FAILURE) 
					{
                        payload.put("error", "Job execution failed");
                    }

                    enqueue(payload);
					return ;
                }

			if (event instanceof FlowableEntityEvent entityEvent &&
				(	type == FlowableEngineEventType.PROCESS_CREATED ||
					type == FlowableEngineEventType.PROCESS_STARTED ||
					type == FlowableEngineEventType.PROCESS_COMPLETED ||
					type == FlowableEngineEventType.PROCESS_CANCELLED || 
					type == FlowableEngineEventType.PROCESS_COMPLETED_WITH_ERROR_END_EVENT ||
					type == FlowableEngineEventType.PROCESS_COMPLETED_WITH_ESCALATION_END_EVENT ||
					type == FlowableEngineEventType.PROCESS_COMPLETED_WITH_TERMINATE_END_EVENT 
				)) 
			{

			payload.put("eventCategory", "PROCESS");
			Object entity = entityEvent.getEntity();

			if (entity instanceof org.flowable.engine.runtime.Execution execution) 
			{

				String processInstanceId = execution.getProcessInstanceId();

				String processDefinitionId = null;

				try 
				{
					ProcessInstance pi = runtimeService
							.createProcessInstanceQuery()
							.processInstanceId(processInstanceId)
							.singleResult();

					if (pi != null) 
					{
						processDefinitionId = pi.getProcessDefinitionId();
					}
				} 
				catch (Exception ignored) {}

				payload.put("processInstanceId", processInstanceId);
				payload.put("executionId", execution.getId());
				payload.put("processDefinitionId", processDefinitionId);
				payload.put("createTime", "");

				enqueue(payload);
				return ;
			}
		}

            // =========================
            // 🔹 ACTIVITY EVENTS
            // =========================
            if (event instanceof FlowableActivityEvent activityEvent) 
			{

                if (type == FlowableEngineEventType.ACTIVITY_STARTED ||
                    type == FlowableEngineEventType.ACTIVITY_COMPLETED||
					type == FlowableEngineEventType.ACTIVITY_SIGNALED  ||
					type == FlowableEngineEventType.ACTIVITY_SIGNAL_WAITING  ||
					type == FlowableEngineEventType.ACTIVITY_MESSAGE_WAITING  ||
					type == FlowableEngineEventType.ACTIVITY_MESSAGE_RECEIVED  ||
					type == FlowableEngineEventType.ACTIVITY_MESSAGE_CANCELLED  ||
					type == FlowableEngineEventType.ACTIVITY_ESCALATION_WAITING  ||
					type == FlowableEngineEventType.ACTIVITY_ESCALATION_RECEIVED  ||
					type == FlowableEngineEventType.ACTIVITY_ERROR_RECEIVED  ||
					type == FlowableEngineEventType.ACTIVITY_CONDITIONAL_WAITING  ||
					type == FlowableEngineEventType.ACTIVITY_CONDITIONAL_RECEIVED  ||
					type == FlowableEngineEventType.ACTIVITY_COMPENSATE  ||
					type == FlowableEngineEventType.ACTIVITY_CANCELLED 
					) {

                    payload.put("eventCategory", "ACTIVITY");
                    payload.put("processInstanceId", activityEvent.getProcessInstanceId());
                    payload.put("executionId", activityEvent.getExecutionId());
                    payload.put("activityId", activityEvent.getActivityId());
                    payload.put("activityName", activityEvent.getActivityName());
					payload.put("createTime", "");
					
					org.flowable.engine.delegate.event.impl.FlowableActivityEventImpl event1 = (org.flowable.engine.delegate.event.impl.FlowableActivityEventImpl) event;
					

                    enqueue(payload);
					return ;
                }
            }

            // =========================
            // 🔹 TASK EVENTS
            // =========================
            else if (event instanceof FlowableEntityEvent entityEvent) {

                Object entity = entityEvent.getEntity();

                if (entity instanceof Task task &&
                    (type == FlowableEngineEventType.TASK_CREATED ||
                     type == FlowableEngineEventType.TASK_ASSIGNED ||
                     type == FlowableEngineEventType.TASK_COMPLETED ||
					 type == FlowableEngineEventType.TASK_DUEDATE_CHANGED ||
					 type == FlowableEngineEventType.TASK_NAME_CHANGED ||
					 type == FlowableEngineEventType.TASK_OWNER_CHANGED ||
					 type == FlowableEngineEventType.TASK_PRIORITY_CHANGED )) 
				{

                    payload.put("eventType", type.name());
                payload.put("eventCategory", "USER_TASK");

                // 🔹 Task info
                payload.put("taskId", task.getId());
                payload.put("taskName", task.getName());
                payload.put("taskDefinitionKey", task.getTaskDefinitionKey());

                // 🔹 User-related fields
                payload.put("assignee", task.getAssignee());
                payload.put("owner", task.getOwner());

                // 🔹 Process context
                payload.put("processInstanceId", task.getProcessInstanceId());
                payload.put("executionId", task.getExecutionId());

                // 🔹 Optional metadata
                payload.put("priority", task.getPriority());
                payload.put("createTime", task.getCreateTime());

                payload.put("timestamp", System.currentTimeMillis());

                    enqueue(payload);
					return ;
                }

                // =========================
                // 🔹 OPTIONAL: EXECUTION (fallback)
                // =========================
                if (entity instanceof Execution execution) {
                    payload.put("executionId", execution.getId());
                    payload.put("processInstanceId", execution.getProcessInstanceId());
					enqueue(payload);
					return ;
                }
            }
 			
			if( event instanceof org.flowable.engine.delegate.event.impl.FlowableActivityCancelledEventImpl )
			{
				org.flowable.engine.delegate.event.impl.FlowableActivityCancelledEventImpl temp = (org.flowable.engine.delegate.event.impl.FlowableActivityCancelledEventImpl) event;
				enqueue(objectMapper.writeValueAsString(temp));
				return;
			}

			if( event instanceof org.flowable.engine.delegate.event.impl.FlowableActivityEventImpl )
			{
				//org.flowable.engine.delegate.event.impl.FlowableActivityEventImpl temp = (org.flowable.engine.delegate.event.impl.FlowableActivityEventImpl) event;
				//kafkaPublisher.publish("loan-events", objectMapper.writeValueAsString(temp));
				return;
			}

			if( event instanceof org.flowable.engine.delegate.event.impl.FlowableConditionalEventImpl )
			{
				org.flowable.engine.delegate.event.impl.FlowableConditionalEventImpl temp = (org.flowable.engine.delegate.event.impl.FlowableConditionalEventImpl) event;
				enqueue(objectMapper.writeValueAsString(temp));
				return;
			}

			if( event instanceof org.flowable.engine.delegate.event.impl.FlowableEntityEventImpl )
			{
				//org.flowable.engine.delegate.event.impl.FlowableEntityEventImpl temp = (org.flowable.engine.delegate.event.impl.FlowableEntityEventImpl) event;
				//kafkaPublisher.publish("loan-events", objectMapper.writeValueAsString(temp));
				return ;
			}

			if( event instanceof org.flowable.engine.delegate.event.impl.FlowableEntityExceptionEventImpl )
			{
				org.flowable.engine.delegate.event.impl.FlowableEntityExceptionEventImpl temp = (org.flowable.engine.delegate.event.impl.FlowableEntityExceptionEventImpl) event;
				enqueue(objectMapper.writeValueAsString(temp));
				return;
			}

			if( event instanceof org.flowable.engine.delegate.event.impl.FlowableEntityWithVariablesEventImpl )
			{
				org.flowable.engine.delegate.event.impl.FlowableEntityWithVariablesEventImpl temp = (org.flowable.engine.delegate.event.impl.FlowableEntityWithVariablesEventImpl) event;
				enqueue(objectMapper.writeValueAsString(temp));
				return;
			}

			if( event instanceof org.flowable.engine.delegate.event.impl.FlowableErrorEventImpl )
			{
				org.flowable.engine.delegate.event.impl.FlowableErrorEventImpl temp = (org.flowable.engine.delegate.event.impl.FlowableErrorEventImpl) event;
				enqueue(objectMapper.writeValueAsString(temp));
				return;
			}

			if( event instanceof org.flowable.engine.delegate.event.impl.FlowableEscalationEventImpl )
			{
				org.flowable.engine.delegate.event.impl.FlowableEscalationEventImpl temp = (org.flowable.engine.delegate.event.impl.FlowableEscalationEventImpl) event;
				enqueue(objectMapper.writeValueAsString(temp));
				return;
			}

			if( event instanceof org.flowable.engine.delegate.event.impl.FlowableJobRescheduledEventImpl )
			{
				org.flowable.engine.delegate.event.impl.FlowableJobRescheduledEventImpl temp = (org.flowable.engine.delegate.event.impl.FlowableJobRescheduledEventImpl) event;
				enqueue(objectMapper.writeValueAsString(temp));
				return;
			}

			if( event instanceof org.flowable.engine.delegate.event.impl.FlowableMessageEventImpl )
			{
				org.flowable.engine.delegate.event.impl.FlowableMessageEventImpl temp = (org.flowable.engine.delegate.event.impl.FlowableMessageEventImpl) event;
				enqueue(objectMapper.writeValueAsString(temp));
				return;
			}

			if( event instanceof org.flowable.engine.delegate.event.impl.FlowableMultiInstanceActivityCancelledEventImpl )
			{
				org.flowable.engine.delegate.event.impl.FlowableMultiInstanceActivityCancelledEventImpl temp = (org.flowable.engine.delegate.event.impl.FlowableMultiInstanceActivityCancelledEventImpl) event;
				enqueue(objectMapper.writeValueAsString(temp));
				return;
			}

			if( event instanceof org.flowable.engine.delegate.event.impl.FlowableMultiInstanceActivityCompletedEventImpl )
			{
				org.flowable.engine.delegate.event.impl.FlowableMultiInstanceActivityCompletedEventImpl temp = (org.flowable.engine.delegate.event.impl.FlowableMultiInstanceActivityCompletedEventImpl) event;
				enqueue(objectMapper.writeValueAsString(temp));
				return;
			}

			if( event instanceof org.flowable.engine.delegate.event.impl.FlowableMultiInstanceActivityEventImpl )
			{
				org.flowable.engine.delegate.event.impl.FlowableMultiInstanceActivityEventImpl temp = (org.flowable.engine.delegate.event.impl.FlowableMultiInstanceActivityEventImpl) event;
				enqueue(objectMapper.writeValueAsString(temp));
				return;
			}

			if( event instanceof org.flowable.engine.delegate.event.impl.FlowableProcessBusinessStatusUpdatedEventImpl )
			{
				org.flowable.engine.delegate.event.impl.FlowableProcessBusinessStatusUpdatedEventImpl temp = (org.flowable.engine.delegate.event.impl.FlowableProcessBusinessStatusUpdatedEventImpl) event;
				enqueue(objectMapper.writeValueAsString(temp));
				return;
			}

			if( event instanceof org.flowable.engine.delegate.event.impl.FlowableProcessCancelledEventImpl )
			{
				org.flowable.engine.delegate.event.impl.FlowableProcessCancelledEventImpl temp = (org.flowable.engine.delegate.event.impl.FlowableProcessCancelledEventImpl) event;
				enqueue(objectMapper.writeValueAsString(temp));
				return;
			}

			if( event instanceof org.flowable.engine.delegate.event.impl.FlowableProcessEventImpl )
			{
				//org.flowable.engine.delegate.event.impl.FlowableProcessEventImpl temp = (org.flowable.engine.delegate.event.impl.FlowableProcessEventImpl) event;
				//enqueue(objectMapper.writeValueAsString(temp));
				return ;
			}

			if( event instanceof org.flowable.engine.delegate.event.impl.FlowableProcessStartedEventImpl )
			{
				org.flowable.engine.delegate.event.impl.FlowableProcessStartedEventImpl temp = (org.flowable.engine.delegate.event.impl.FlowableProcessStartedEventImpl) event;
				enqueue(objectMapper.writeValueAsString(temp));
				return;
			}

			if( event instanceof org.flowable.engine.delegate.event.impl.FlowableProcessTerminatedEventImpl )
			{
				org.flowable.engine.delegate.event.impl.FlowableProcessTerminatedEventImpl temp = (org.flowable.engine.delegate.event.impl.FlowableProcessTerminatedEventImpl) event;
				enqueue(objectMapper.writeValueAsString(temp));
				return;
			}

			if( event instanceof org.flowable.engine.delegate.event.impl.FlowableSequenceFlowTakenEventImpl )
			{
				org.flowable.engine.delegate.event.impl.FlowableSequenceFlowTakenEventImpl temp = (org.flowable.engine.delegate.event.impl.FlowableSequenceFlowTakenEventImpl) event;
				enqueue(objectMapper.writeValueAsString(temp));
				return;
			}

			if( event instanceof org.flowable.engine.delegate.event.impl.FlowableSignalEventImpl )
			{
				org.flowable.engine.delegate.event.impl.FlowableSignalEventImpl temp = (org.flowable.engine.delegate.event.impl.FlowableSignalEventImpl) event;
				enqueue(objectMapper.writeValueAsString(temp));
				return;
			}

			if( event instanceof org.flowable.engine.delegate.event.impl.FlowableVariableEventImpl )
			{
				org.flowable.engine.delegate.event.impl.FlowableVariableEventImpl temp = (org.flowable.engine.delegate.event.impl.FlowableVariableEventImpl) event;
				enqueue(objectMapper.writeValueAsString(temp));
				return;
			}

			System.out.println("Event class: " + event.getClass() + " = "+event.getType());	

        } catch (Exception e) {
            // ⚠️ never throw exception back to Flowable
            e.printStackTrace();
        }
    }

    /**
     * ✅ Central enqueue method (FAST + SAFE)
     */
    private void enqueue(Map<String, Object> payload) {
        try {
            String json = objectMapper.writeValueAsString(payload);

            boolean accepted = queueService.publish(json);

            if (!accepted) {
                // optional: count drops (avoid logging here)
            }
			payload.clear();
        } 
		catch (Exception ignored) 
		{
        }
    }

    private void enqueue(String json) {
        try {

            boolean accepted = queueService.publish(json);

            if (!accepted) {
                // optional: count drops (avoid logging here)
            }

        } catch (Exception ignored) {
        }
    }


    @Override
    public boolean isFailOnException() {
        return false;
    }

    @Override
    public boolean isFireOnTransactionLifecycleEvent() {
        return false;
    }

    @Override
    public String getOnTransaction() {
        return "COMMITTED";
    }

    
    public boolean isSync() {
        return true;
    }
}

/*
package com.example.flowable.listener;

import com.example.flowable.kafka.KafkaPublisher;

import org.flowable.common.engine.api.delegate.event.FlowableEventListener;
import org.flowable.engine.delegate.event.impl.FlowableEntityEventImpl;
import org.springframework.stereotype.Component;
import org.flowable.common.engine.api.delegate.event.FlowableEngineEventType;
import org.flowable.common.engine.api.delegate.event.FlowableEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import java.util.HashMap;
import java.util.Map;
import org.flowable.common.engine.api.delegate.event.FlowableEvent;
import org.flowable.common.engine.api.delegate.event.FlowableEntityEvent;
import org.flowable.common.engine.api.delegate.event.FlowableEngineEventType;
import org.flowable.engine.RuntimeService;
import org.flowable.engine.runtime.Execution;
import org.flowable.engine.runtime.ProcessInstance;
import org.flowable.task.api.Task;
import org.flowable.engine.delegate.event.FlowableActivityEvent;

import org.flowable.engine.delegate.event.impl.FlowableActivityCancelledEventImpl;
import org.flowable.engine.delegate.event.impl.FlowableActivityEventImpl;
import org.flowable.engine.delegate.event.impl.FlowableConditionalEventImpl;
import org.flowable.engine.delegate.event.impl.FlowableEntityEventImpl;
import org.flowable.engine.delegate.event.impl.FlowableEntityExceptionEventImpl;
import org.flowable.engine.delegate.event.impl.FlowableEntityWithVariablesEventImpl;
import org.flowable.engine.delegate.event.impl.FlowableErrorEventImpl;
import org.flowable.engine.delegate.event.impl.FlowableEscalationEventImpl;
import org.flowable.engine.delegate.event.impl.FlowableJobRescheduledEventImpl;
import org.flowable.engine.delegate.event.impl.FlowableMessageEventImpl;
import org.flowable.engine.delegate.event.impl.FlowableMultiInstanceActivityCancelledEventImpl;
import org.flowable.engine.delegate.event.impl.FlowableMultiInstanceActivityCompletedEventImpl;
import org.flowable.engine.delegate.event.impl.FlowableMultiInstanceActivityEventImpl;
import org.flowable.engine.delegate.event.impl.FlowableProcessBusinessStatusUpdatedEventImpl;
import org.flowable.engine.delegate.event.impl.FlowableProcessCancelledEventImpl;
import org.flowable.engine.delegate.event.impl.FlowableProcessEventImpl;
import org.flowable.engine.delegate.event.impl.FlowableProcessStartedEventImpl;
import org.flowable.engine.delegate.event.impl.FlowableProcessTerminatedEventImpl;
import org.flowable.engine.delegate.event.impl.FlowableSequenceFlowTakenEventImpl;
import org.flowable.engine.delegate.event.impl.FlowableSignalEventImpl;
import org.flowable.engine.delegate.event.impl.FlowableVariableEventImpl;



@Component
public class GlobalFlowableListener implements FlowableEventListener {
	@Autowired
    private RuntimeService runtimeService;
	
    private final KafkaPublisher kafkaPublisher;

    public GlobalFlowableListener(KafkaPublisher kafkaPublisher) {
        this.kafkaPublisher = kafkaPublisher;
    }



	@Override
	public void onEvent(FlowableEvent event) 
	{
		//System.out.println("RAW EVENT => " + event.getType() + " "+event.toString());
		
		
		try 
		{

            ObjectMapper objectMapper = new ObjectMapper();
			FlowableEngineEventType type = (FlowableEngineEventType) event.getType();
			
			if( type == FlowableEngineEventType.ENTITY_ACTIVATED ||
				type == FlowableEngineEventType.ENTITY_CREATED ||
				type == FlowableEngineEventType.ENTITY_DELETED ||
				type == FlowableEngineEventType.ENTITY_INITIALIZED ||
				type == FlowableEngineEventType.ENTITY_SUSPENDED ||
				type == FlowableEngineEventType.ENTITY_UPDATED 
				)
				return ;

			Map<String, Object> payload = new HashMap<>();
			payload.put("eventType", type.name());
			payload.put("timestamp", System.currentTimeMillis());
			
			
			if (event instanceof  org.flowable.engine.delegate.event.FlowableSequenceFlowTakenEvent activityEvent) 
			{
                if (type == FlowableEngineEventType.SEQUENCEFLOW_TAKEN )
				{					
					payload.put("id", activityEvent.getId());
					payload.put("sourceActivityBehaviorClass",activityEvent.getSourceActivityBehaviorClass());
					payload.put("sourceActivityId",activityEvent.getSourceActivityId());
					payload.put("sourceActivityName",activityEvent.getSourceActivityName());
					payload.put("sourceActivityType",activityEvent.getSourceActivityType());
					payload.put("targetActivityBehaviorClass",activityEvent.getTargetActivityBehaviorClass());
					payload.put("targetActivityId",activityEvent.getTargetActivityId());
					payload.put("targetActivityName",activityEvent.getTargetActivityName());
					payload.put("targetActivityType" ,activityEvent.getTargetActivityType()); 
					payload.put("executionId",activityEvent.getExecutionId());
					payload.put("processDefinitionId",activityEvent.getProcessDefinitionId());
					payload.put("processInstanceId",activityEvent.getProcessInstanceId());
					payload.put("scopeDefinitionId",activityEvent.getScopeDefinitionId());
					payload.put("scopeId",activityEvent.getScopeId());
					payload.put("scopeType",activityEvent.getScopeType());
					payload.put("subScopeId",activityEvent.getSubScopeId());					
					enqueue(objectMapper.writeValueAsString(payload));
				   return ;					
				}
			}
			
            // =========================
            // 🔹 SERVICE TASK (ACTIVITY)
            // =========================
            if (event instanceof FlowableActivityEvent activityEvent) {

                String activityType = activityEvent.getActivityType();

                if ("serviceTask".equals(activityType) &&
                    (type == FlowableEngineEventType.ACTIVITY_STARTED ||
                     type == FlowableEngineEventType.ACTIVITY_COMPLETED)) {

                    payload.put("eventCategory", "SERVICE_TASK");
                    payload.put("status",type == FlowableEngineEventType.ACTIVITY_STARTED ? "STARTED" : "COMPLETED");

                    payload.put("processInstanceId", activityEvent.getProcessInstanceId());
                    payload.put("executionId", activityEvent.getExecutionId());
                    payload.put("activityId", activityEvent.getActivityId());
                    payload.put("activityName", activityEvent.getActivityName());
                    payload.put("activityType", activityType);

                   enqueue(objectMapper.writeValueAsString(payload));
				   return ;
                }
            }

            // =========================
            // 🔹 SERVICE TASK ERRORS (ASYNC JOB)
            // =========================
            else  if (type == FlowableEngineEventType.JOB_CANCELED  ||
						type == FlowableEngineEventType.JOB_EXECUTION_FAILURE  ||
						type == FlowableEngineEventType.JOB_EXECUTION_SUCCESS  ||
						type == FlowableEngineEventType.JOB_MOVED_TO_DEADLETTER  ||
						type == FlowableEngineEventType.JOB_REJECTED ||
						type == FlowableEngineEventType.JOB_RESCHEDULED  ||
						type == FlowableEngineEventType.JOB_RETRIES_DECREMENTED ) 
					{
					 
                    payload.put("eventCategory", "SERVICE_TASK_JOB");


                    payload.put("status",type == FlowableEngineEventType.JOB_EXECUTION_SUCCESS ? "COMPLETED" : "FAILED");

                    if (type == FlowableEngineEventType.JOB_EXECUTION_FAILURE) 
					{
                        payload.put("error", "Job execution failed");
                    }

                    enqueue(objectMapper.writeValueAsString(payload));
					return ;
                }

			if (event instanceof FlowableEntityEvent entityEvent &&
				(	type == FlowableEngineEventType.PROCESS_CREATED ||
					type == FlowableEngineEventType.PROCESS_STARTED ||
					type == FlowableEngineEventType.PROCESS_COMPLETED ||
					type == FlowableEngineEventType.PROCESS_CANCELLED || 
					type == FlowableEngineEventType.PROCESS_COMPLETED_WITH_ERROR_END_EVENT ||
					type == FlowableEngineEventType.PROCESS_COMPLETED_WITH_ESCALATION_END_EVENT ||
					type == FlowableEngineEventType.PROCESS_COMPLETED_WITH_TERMINATE_END_EVENT 
				)) 
			{

			payload.put("eventCategory", "PROCESS");
			Object entity = entityEvent.getEntity();

			if (entity instanceof org.flowable.engine.runtime.Execution execution) 
			{

				String processInstanceId = execution.getProcessInstanceId();

				String processDefinitionId = null;

				try 
				{
					ProcessInstance pi = runtimeService
							.createProcessInstanceQuery()
							.processInstanceId(processInstanceId)
							.singleResult();

					if (pi != null) 
					{
						processDefinitionId = pi.getProcessDefinitionId();
					}
				} 
				catch (Exception ignored) {}

				payload.put("processInstanceId", processInstanceId);
				payload.put("executionId", execution.getId());
				payload.put("processDefinitionId", processDefinitionId);
				payload.put("createTime", "");

				enqueue(objectMapper.writeValueAsString(payload));
				return ;
			}
		}

            // =========================
            // 🔹 ACTIVITY EVENTS
            // =========================
            if (event instanceof FlowableActivityEvent activityEvent) 
			{

                if (type == FlowableEngineEventType.ACTIVITY_STARTED ||
                    type == FlowableEngineEventType.ACTIVITY_COMPLETED||
					type == FlowableEngineEventType.ACTIVITY_SIGNALED  ||
					type == FlowableEngineEventType.ACTIVITY_SIGNAL_WAITING  ||
					type == FlowableEngineEventType.ACTIVITY_MESSAGE_WAITING  ||
					type == FlowableEngineEventType.ACTIVITY_MESSAGE_RECEIVED  ||
					type == FlowableEngineEventType.ACTIVITY_MESSAGE_CANCELLED  ||
					type == FlowableEngineEventType.ACTIVITY_ESCALATION_WAITING  ||
					type == FlowableEngineEventType.ACTIVITY_ESCALATION_RECEIVED  ||
					type == FlowableEngineEventType.ACTIVITY_ERROR_RECEIVED  ||
					type == FlowableEngineEventType.ACTIVITY_CONDITIONAL_WAITING  ||
					type == FlowableEngineEventType.ACTIVITY_CONDITIONAL_RECEIVED  ||
					type == FlowableEngineEventType.ACTIVITY_COMPENSATE  ||
					type == FlowableEngineEventType.ACTIVITY_CANCELLED 
					) {

                    payload.put("eventCategory", "ACTIVITY");
                    payload.put("processInstanceId", activityEvent.getProcessInstanceId());
                    payload.put("executionId", activityEvent.getExecutionId());
                    payload.put("activityId", activityEvent.getActivityId());
                    payload.put("activityName", activityEvent.getActivityName());
					payload.put("createTime", "");
					
					org.flowable.engine.delegate.event.impl.FlowableActivityEventImpl event1 = (org.flowable.engine.delegate.event.impl.FlowableActivityEventImpl) event;
					

                    enqueue(objectMapper.writeValueAsString(payload));
					return ;
                }
            }

            // =========================
            // 🔹 TASK EVENTS
            // =========================
            else if (event instanceof FlowableEntityEvent entityEvent) {

                Object entity = entityEvent.getEntity();

                if (entity instanceof Task task &&
                    (type == FlowableEngineEventType.TASK_CREATED ||
                     type == FlowableEngineEventType.TASK_ASSIGNED ||
                     type == FlowableEngineEventType.TASK_COMPLETED ||
					 type == FlowableEngineEventType.TASK_DUEDATE_CHANGED ||
					 type == FlowableEngineEventType.TASK_NAME_CHANGED ||
					 type == FlowableEngineEventType.TASK_OWNER_CHANGED ||
					 type == FlowableEngineEventType.TASK_PRIORITY_CHANGED )) 
				{

                    payload.put("eventType", type.name());
                payload.put("eventCategory", "USER_TASK");

                // 🔹 Task info
                payload.put("taskId", task.getId());
                payload.put("taskName", task.getName());
                payload.put("taskDefinitionKey", task.getTaskDefinitionKey());

                // 🔹 User-related fields
                payload.put("assignee", task.getAssignee());
                payload.put("owner", task.getOwner());

                // 🔹 Process context
                payload.put("processInstanceId", task.getProcessInstanceId());
                payload.put("executionId", task.getExecutionId());

                // 🔹 Optional metadata
                payload.put("priority", task.getPriority());
                payload.put("createTime", task.getCreateTime());

                payload.put("timestamp", System.currentTimeMillis());

                    enqueue(objectMapper.writeValueAsString(payload));
					return ;
                }

                // =========================
                // 🔹 OPTIONAL: EXECUTION (fallback)
                // =========================
                if (entity instanceof Execution execution) {
                    payload.put("executionId", execution.getId());
                    payload.put("processInstanceId", execution.getProcessInstanceId());
					enqueue(objectMapper.writeValueAsString(payload));
					return ;
                }
            }
 			
			if( event instanceof org.flowable.engine.delegate.event.impl.FlowableActivityCancelledEventImpl )
			{
				org.flowable.engine.delegate.event.impl.FlowableActivityCancelledEventImpl temp = (org.flowable.engine.delegate.event.impl.FlowableActivityCancelledEventImpl) event;
				enqueue(objectMapper.writeValueAsString(temp));
			}

			if( event instanceof org.flowable.engine.delegate.event.impl.FlowableActivityEventImpl )
			{
				//org.flowable.engine.delegate.event.impl.FlowableActivityEventImpl temp = (org.flowable.engine.delegate.event.impl.FlowableActivityEventImpl) event;
				//enqueue(objectMapper.writeValueAsString(temp));
				return;
			}

			if( event instanceof org.flowable.engine.delegate.event.impl.FlowableConditionalEventImpl )
			{
				org.flowable.engine.delegate.event.impl.FlowableConditionalEventImpl temp = (org.flowable.engine.delegate.event.impl.FlowableConditionalEventImpl) event;
				enqueue(objectMapper.writeValueAsString(temp));
			}

			if( event instanceof org.flowable.engine.delegate.event.impl.FlowableEntityEventImpl )
			{
				//org.flowable.engine.delegate.event.impl.FlowableEntityEventImpl temp = (org.flowable.engine.delegate.event.impl.FlowableEntityEventImpl) event;
				//enqueue(objectMapper.writeValueAsString(temp));
				return ;
			}

			if( event instanceof org.flowable.engine.delegate.event.impl.FlowableEntityExceptionEventImpl )
			{
				org.flowable.engine.delegate.event.impl.FlowableEntityExceptionEventImpl temp = (org.flowable.engine.delegate.event.impl.FlowableEntityExceptionEventImpl) event;
				enqueue(objectMapper.writeValueAsString(temp));
			}

			if( event instanceof org.flowable.engine.delegate.event.impl.FlowableEntityWithVariablesEventImpl )
			{
				org.flowable.engine.delegate.event.impl.FlowableEntityWithVariablesEventImpl temp = (org.flowable.engine.delegate.event.impl.FlowableEntityWithVariablesEventImpl) event;
				enqueue(objectMapper.writeValueAsString(temp));
			}

			if( event instanceof org.flowable.engine.delegate.event.impl.FlowableErrorEventImpl )
			{
				org.flowable.engine.delegate.event.impl.FlowableErrorEventImpl temp = (org.flowable.engine.delegate.event.impl.FlowableErrorEventImpl) event;
				enqueue(objectMapper.writeValueAsString(temp));
			}

			if( event instanceof org.flowable.engine.delegate.event.impl.FlowableEscalationEventImpl )
			{
				org.flowable.engine.delegate.event.impl.FlowableEscalationEventImpl temp = (org.flowable.engine.delegate.event.impl.FlowableEscalationEventImpl) event;
				enqueue(objectMapper.writeValueAsString(temp));
			}

			if( event instanceof org.flowable.engine.delegate.event.impl.FlowableJobRescheduledEventImpl )
			{
				org.flowable.engine.delegate.event.impl.FlowableJobRescheduledEventImpl temp = (org.flowable.engine.delegate.event.impl.FlowableJobRescheduledEventImpl) event;
				enqueue(objectMapper.writeValueAsString(temp));
			}

			if( event instanceof org.flowable.engine.delegate.event.impl.FlowableMessageEventImpl )
			{
				org.flowable.engine.delegate.event.impl.FlowableMessageEventImpl temp = (org.flowable.engine.delegate.event.impl.FlowableMessageEventImpl) event;
				enqueue(objectMapper.writeValueAsString(temp));
			}

			if( event instanceof org.flowable.engine.delegate.event.impl.FlowableMultiInstanceActivityCancelledEventImpl )
			{
				org.flowable.engine.delegate.event.impl.FlowableMultiInstanceActivityCancelledEventImpl temp = (org.flowable.engine.delegate.event.impl.FlowableMultiInstanceActivityCancelledEventImpl) event;
				enqueue(objectMapper.writeValueAsString(temp));
			}

			if( event instanceof org.flowable.engine.delegate.event.impl.FlowableMultiInstanceActivityCompletedEventImpl )
			{
				org.flowable.engine.delegate.event.impl.FlowableMultiInstanceActivityCompletedEventImpl temp = (org.flowable.engine.delegate.event.impl.FlowableMultiInstanceActivityCompletedEventImpl) event;
				enqueue(objectMapper.writeValueAsString(temp));
			}

			if( event instanceof org.flowable.engine.delegate.event.impl.FlowableMultiInstanceActivityEventImpl )
			{
				org.flowable.engine.delegate.event.impl.FlowableMultiInstanceActivityEventImpl temp = (org.flowable.engine.delegate.event.impl.FlowableMultiInstanceActivityEventImpl) event;
				enqueue(objectMapper.writeValueAsString(temp));
			}

			if( event instanceof org.flowable.engine.delegate.event.impl.FlowableProcessBusinessStatusUpdatedEventImpl )
			{
				org.flowable.engine.delegate.event.impl.FlowableProcessBusinessStatusUpdatedEventImpl temp = (org.flowable.engine.delegate.event.impl.FlowableProcessBusinessStatusUpdatedEventImpl) event;
				enqueue(objectMapper.writeValueAsString(temp));
			}

			if( event instanceof org.flowable.engine.delegate.event.impl.FlowableProcessCancelledEventImpl )
			{
				org.flowable.engine.delegate.event.impl.FlowableProcessCancelledEventImpl temp = (org.flowable.engine.delegate.event.impl.FlowableProcessCancelledEventImpl) event;
				enqueue(objectMapper.writeValueAsString(temp));
			}

			if( event instanceof org.flowable.engine.delegate.event.impl.FlowableProcessEventImpl )
			{
				//org.flowable.engine.delegate.event.impl.FlowableProcessEventImpl temp = (org.flowable.engine.delegate.event.impl.FlowableProcessEventImpl) event;
				//enqueue(objectMapper.writeValueAsString(temp));
				return ;
			}

			if( event instanceof org.flowable.engine.delegate.event.impl.FlowableProcessStartedEventImpl )
			{
				org.flowable.engine.delegate.event.impl.FlowableProcessStartedEventImpl temp = (org.flowable.engine.delegate.event.impl.FlowableProcessStartedEventImpl) event;
				enqueue(objectMapper.writeValueAsString(temp));
			}

			if( event instanceof org.flowable.engine.delegate.event.impl.FlowableProcessTerminatedEventImpl )
			{
				org.flowable.engine.delegate.event.impl.FlowableProcessTerminatedEventImpl temp = (org.flowable.engine.delegate.event.impl.FlowableProcessTerminatedEventImpl) event;
				enqueue(objectMapper.writeValueAsString(temp));
			}

			if( event instanceof org.flowable.engine.delegate.event.impl.FlowableSequenceFlowTakenEventImpl )
			{
				org.flowable.engine.delegate.event.impl.FlowableSequenceFlowTakenEventImpl temp = (org.flowable.engine.delegate.event.impl.FlowableSequenceFlowTakenEventImpl) event;
				enqueue(objectMapper.writeValueAsString(temp));
			}

			if( event instanceof org.flowable.engine.delegate.event.impl.FlowableSignalEventImpl )
			{
				org.flowable.engine.delegate.event.impl.FlowableSignalEventImpl temp = (org.flowable.engine.delegate.event.impl.FlowableSignalEventImpl) event;
				enqueue(objectMapper.writeValueAsString(temp));
			}

			if( event instanceof org.flowable.engine.delegate.event.impl.FlowableVariableEventImpl )
			{
				org.flowable.engine.delegate.event.impl.FlowableVariableEventImpl temp = (org.flowable.engine.delegate.event.impl.FlowableVariableEventImpl) event;
				enqueue(objectMapper.writeValueAsString(temp));
			}

			System.out.println("Event class: " + event.getClass() + " = "+event.getType());	
        } 
		catch (Exception e) 
		{
            System.out.println("*** Exception *** START *** --------------------------------------------------" );
			e.printStackTrace();
			System.out.println("*** Exception *** END   *** --------------------------------------------------" );
        }
	}

    @Override
    public boolean isFailOnException() {
        return false;
    }

    @Override
    public boolean isFireOnTransactionLifecycleEvent() {
        return false;
    }

    @Override
    public String getOnTransaction() {
        return "COMMITTED";
    }

   
    public boolean isSync() {
        return true;
    }

	private String buildJson(String eventType, String processInstanceId, String processDefinitionId,String executionId,String name) 
	{

		return "{"
			+ "\"eventType\":\"" + eventType + "\","
			+ "\"processInstanceId\":\"" + processInstanceId + "\","
			+ "\"processDefinitionId\":\"" + processDefinitionId + "\","
			+ "\"executionId\":\"" + executionId + "\","
			+ "\"name\":\"" + name + "\""
			+ "}";
	}
}
*/