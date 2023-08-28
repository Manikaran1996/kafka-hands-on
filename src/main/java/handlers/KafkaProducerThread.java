package handlers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import serializer.JsonSerializer;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

public class KafkaProducerThread implements Runnable {

    private final KafkaProducer<String, JsonNode> kafkaProducer;
    private final int numberOfProduces;
    private final int timeBetweenConsecutiveProduceMs;
    private final ObjectMapper objectMapper;
    private final String topic;


    public KafkaProducerThread(int numberOfProduces, int timeBetweenConsecutiveProduceMs, String topic) {
        System.out.println("Initiating Kafka producer");
        Properties producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProperties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "250");
        producerProperties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "100");
        kafkaProducer = new KafkaProducer<>(producerProperties);
        this.numberOfProduces = numberOfProduces;
        this.timeBetweenConsecutiveProduceMs = timeBetweenConsecutiveProduceMs;
        objectMapper = new ObjectMapper();
        this.topic = topic;
    }

    @Override
    public void run() {
        int produceCount = 0;
        Random randomGen = new Random(Instant.now().getEpochSecond());
        while (produceCount < numberOfProduces) {
            Map<String, Integer> map = new HashMap<>();
            map.put("A", randomGen.nextInt());
            map.put("B", randomGen.nextInt());
            JsonNode jsonNode = objectMapper.valueToTree(map);
            kafkaProducer.send(new ProducerRecord<String, JsonNode>(topic, "ProduceNumber:" + produceCount, jsonNode));
            try {
                Thread.sleep(timeBetweenConsecutiveProduceMs);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            ++produceCount;
        }

    }
}
