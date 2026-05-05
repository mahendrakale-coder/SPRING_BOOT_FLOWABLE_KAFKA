package com.example.flowable.kafka;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import java.util.ArrayList;
import java.util.List;

@Service
public class KafkaPublisher {

    private static final String TOPIC = "loan-events";
    private static final int WORKER_COUNT = 8;
    private static final int BATCH_SIZE = 1;
	

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private EventQueueService queueService;

    @PostConstruct
    public void start() {
        for (int i = 0; i < WORKER_COUNT; i++) {
            Thread t = new Thread(this::run, "kafka-publisher-" + i);
            t.setDaemon(true);
            t.start();
        }
    }

    private void run() {
		System.out.println("Kafka worker started: " + Thread.currentThread().getName());
        while (true) {
            try {
                List<String> batch = new ArrayList<>(BATCH_SIZE);

                int drained = queueService.drainTo(batch, BATCH_SIZE);

                if (drained == 0) {
                    // prevent CPU spinning
					System.out.println("Kafka worker sleeping : " + Thread.currentThread().getName());
                    Thread.sleep(5);
                    continue;
                }
				System.out.println("KafkaPublisher.java : batch.size() : "+batch.size());
                for (String event : batch) {
					System.out.println("KafkaPublisher.java : event : "+event);
                    kafkaTemplate.send(TOPIC, event);
                }

            } catch (Exception e) {
                // NEVER crash the worker thread
                e.printStackTrace();
            }
        }
    }
}

/*@Service
public class KafkaPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
	
	@Autowired
    private EventQueueService queueService;
	
    public KafkaPublisher(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(String topic, String message) 
	{
		
        kafkaTemplate.send(topic, message,message).whenComplete((result, ex) -> {
						if (ex == null) {
							System.out.println("SENT MESSAGE TO KAFKA SUCCESSFULESS => " + message);
							System.out.println("Message sent: " + result.getRecordMetadata());
						} else {
							System.out.println("SEND MESSAGE TO KAFKA FAILED        => " + message);
							ex.printStackTrace();
						}
		});
    }	
}*/