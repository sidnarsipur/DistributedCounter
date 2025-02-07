import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Properties;
import java.util.UUID;

public class DistroCounter {
    private final ConcurrentHashMap<String, Long> counters = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> processedTokens = new ConcurrentHashMap<>();
    private final KafkaProducer<String, String> producer;
    private final String KAFKA_TOPIC = "counter-events";

    public DistroCounter() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all");
        this.producer = new KafkaProducer<>(props);
    }

    public synchronized Long incrementCounter(String counterName, String idempotencyToken) {
        if (processedTokens.containsKey(idempotencyToken)) {
            return counters.get(counterName);
        }

        Long newValue = counters.compute(counterName, (k, v) -> (v == null) ? 1L : v + 1L);

        String eventId = UUID.randomUUID().toString();
        String eventData = String.format("{\"counterName\":\"%s\",\"operation\":\"increment\",\"newValue\":%d,\"token\":\"%s\"}",
                counterName, newValue, idempotencyToken);

        ProducerRecord<String, String> record = new ProducerRecord<>(KAFKA_TOPIC, eventId, eventData);
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                System.err.println("Failed to log event to Kafka: " + exception.getMessage());
            }
        });

        processedTokens.put(idempotencyToken, counterName);
        return newValue;
    }

    public Long getCounter(String counterName) {
        return counters.getOrDefault(counterName, 0L);
    }

    public void close() {
        producer.close();
    }

    public static void main(String[] args) {
        DistroCounter counterService = new DistroCounter();
        
        String counterId = "test-counter";
        String idempotencyToken = UUID.randomUUID().toString();
        
        try {
            Long value = counterService.incrementCounter(counterId, idempotencyToken);
            System.out.println("Counter value: " + value);
            
            Long secondValue = counterService.incrementCounter(counterId, idempotencyToken);
            System.out.println("Counter value (same token): " + secondValue);
            
            String newToken = UUID.randomUUID().toString();
            Long thirdValue = counterService.incrementCounter(counterId, newToken);
            System.out.println("Counter value (new token): " + thirdValue);
        } finally {
            counterService.close();
        }
    }
} 