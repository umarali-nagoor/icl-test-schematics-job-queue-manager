package com.ibm.schematics.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JobConsumer {
    private static final Logger log = LoggerFactory.getLogger(JobConsumer.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    @KafkaListener(topics = "schematics-jobs")
    public void onMessage(JobMessage message) throws Exception {
        log.info("Received job message {}", message);
        log.info("Job spec: " + objectMapper.writeValueAsString(message.getSpec()));
        queue.enqueue(message);
        log.info("Enqueued job {} depth={} items", message.getJobId(), queue.depth());
    }

    @Scheduled(fixedRate = 1000)
    public void drainQueue() {
        log.info("Draining queue");
        queue.drain();
    }
}
