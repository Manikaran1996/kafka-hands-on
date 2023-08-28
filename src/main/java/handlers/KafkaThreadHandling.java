package handlers;

import com.fasterxml.jackson.databind.JsonNode;

public class KafkaThreadHandling implements Runnable {

    private final JsonNode message;
    private final String key;

    public KafkaThreadHandling(String key, JsonNode message) {
        System.out.println("Starting thread to handle request");
        this.message = message;
        this.key = key;
    }

    @Override
    public void run() {
        int first = this.message.get("A").asInt();
        int second = this.message.get("B").asInt();
        int total = first + second;
        System.out.println("Key = " + key + ", First = " + first + ", Second = " + second + " and total = " + total);
    }
}
