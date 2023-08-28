import com.fasterxml.jackson.databind.JsonNode;
import handlers.KafkaConsumerThread;
import handlers.KafkaProducerThread;
import handlers.KafkaThreadHandling;
import model.Pair;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class KafkaConsumerWithThreadsHandlingMessages {

    public static void main(String[] args) throws InterruptedException {
        ThreadPoolExecutor kafkaMessageHandlerThreadPool = (ThreadPoolExecutor) Executors.newCachedThreadPool();
        ExecutorService kafkaConsumerThreadPool = Executors.newSingleThreadExecutor();
        ExecutorService kafkaProducerThreadPool = Executors.newSingleThreadExecutor();
        ArrayBlockingQueue<Pair<String,JsonNode>> queue = new ArrayBlockingQueue<>(100);

        kafkaProducerThreadPool.execute(new KafkaProducerThread(1000, 100, "test.mk.1"));
        Thread.sleep(5000);
        kafkaConsumerThreadPool.execute(new KafkaConsumerThread(queue));

        while (true) {
            Pair<String, JsonNode> dataToProcess = queue.poll();
            if (dataToProcess != null) {
                kafkaMessageHandlerThreadPool.execute(new KafkaThreadHandling(dataToProcess.getFirst(),
                        dataToProcess.getSecond()));
            }
        }
    }


}
