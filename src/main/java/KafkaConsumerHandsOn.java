import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

public class KafkaConsumerHandsOn {

    public static void main(String[] args) throws IOException {

        Properties properties = new Properties();

        FileInputStream fis = new FileInputStream(KafkaConsumerHandsOn.class.getResource("config.properties").getPath());

        properties.load(fis);

        String bootstrapServer = properties.getProperty("bootstrap_server");
        String topicsRegex = properties.getProperty("topics_regex");

        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,");
        kafkaProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "mkTest");
        kafkaProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaProperties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(kafkaProperties);
        kafkaConsumer.subscribe(Pattern.compile(".*mani.*"));

        while (true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(60));
            if (consumerRecords.isEmpty())
                continue;
            for (ConsumerRecord<String, String> record : consumerRecords) {
                System.out.println("------------- New Record -----------");
                System.out.print("Topic, Partition, Offset, Timestamp: ");
                System.out.println(record.topic() + ", " + record.partition() + ", " + record.offset() + ", " + record.timestamp());
                System.out.print("Key: ");
                System.out.println(record.key());
                System.out.print("Value: ");
                System.out.println(record.value());
                System.out.print("Headers: ");
                System.out.println(record.headers());
            }
            kafkaConsumer.commitAsync(new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                    System.out.println("Offsets committed");
                    for (Map.Entry<TopicPartition, OffsetAndMetadata> entry: map.entrySet()) {
                        TopicPartition topicPartition = entry.getKey();
                        System.out.print("TopicPartition: ");
                        System.out.println(topicPartition.toString());
                        OffsetAndMetadata offsetAndMetadata = entry.getValue();
                        System.out.print("Offsets: ");
                        System.out.println(offsetAndMetadata.offset());
                        System.out.print("Metadata: ");
                        System.out.println(offsetAndMetadata.metadata());
                    }
                }
            });

        }




    }


}
