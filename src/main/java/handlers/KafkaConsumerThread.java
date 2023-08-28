package handlers;

import com.fasterxml.jackson.databind.JsonNode;
import model.Pair;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import serializer.JsonDeserializer;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.regex.Pattern;

public class KafkaConsumerThread implements Runnable {

    private final BlockingQueue<Pair<String,JsonNode>> msgQueue;
    private final static Integer POLL_DURATION_SECONDS = 30;
    private final KafkaConsumer<String, JsonNode>  consumer;

    public KafkaConsumerThread(BlockingQueue<Pair<String,JsonNode>> queue) {
        System.out.println("Initiating Kafka Consumer");
        msgQueue = queue;
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        kafkaProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "mk-consumer-" + Instant.now());
        kafkaProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        consumer = new KafkaConsumer<>(kafkaProperties);
        consumer.subscribe(Pattern.compile("test.mk.*"));
    }


    @Override
    public void run() {

        while (true) {
            ConsumerRecords<String, JsonNode> records = consumer.poll(Duration.ofSeconds(POLL_DURATION_SECONDS));
            if (records.count() != 0) {
                for (ConsumerRecord<String, JsonNode> consumerRecord : records) {
                    String msgKey = consumerRecord.key();
                    JsonNode data = consumerRecord.value();
                    System.out.println("Processing message key: " + msgKey);
                    if (data != null) {
                        msgQueue.add(new Pair<>(msgKey, data));
                    }

                }
            }
        }



    }
}
