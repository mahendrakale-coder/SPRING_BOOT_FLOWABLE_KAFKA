package com.example.flowable.kafka;

import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class EventQueueService {

    // Tune this based on memory + traffic
    private static final int QUEUE_CAPACITY = 100_000;

    private final BlockingQueue<String> queue =
            new ArrayBlockingQueue<>(QUEUE_CAPACITY);

    // Metrics (optional but useful in production)
    private final AtomicLong acceptedCount = new AtomicLong(0);
    private final AtomicLong droppedCount = new AtomicLong(0);

    /**
     * Non-blocking publish.
     * Returns true if accepted, false if queue is full.
     */
    public boolean publish(String event) {
		System.out.println("EventQueueService.java : "+event);
        boolean offered = queue.offer(event);

        if (offered) {
            acceptedCount.incrementAndGet();
        } else {
            droppedCount.incrementAndGet();
        }

        return offered;
    }

    /**
     * Drain events into a batch (used by Kafka workers).
     */
    public int drainTo(List<String> target, int maxBatchSize) {
		
        return queue.drainTo(target, maxBatchSize);
    }

    /**
     * Optional: take single event (blocking) — usually NOT used in high-throughput mode
     */
    public String take() throws InterruptedException {
        return queue.take();
    }

    /**
     * Current queue size (for monitoring)
     */
    public int size() {
        return queue.size();
    }

    /**
     * Remaining capacity (helps detect pressure)
     */
    public int remainingCapacity() {
        return queue.remainingCapacity();
    }

    /**
     * Total accepted events
     */
    public long getAcceptedCount() {
        return acceptedCount.get();
    }

    /**
     * Total dropped events (IMPORTANT metric)
     */
    public long getDroppedCount() {
        return droppedCount.get();
    }

    /**
     * Simple health indicator
     */
    public boolean isNearCapacity() {
        return queue.size() > (QUEUE_CAPACITY * 0.8);
    }
}