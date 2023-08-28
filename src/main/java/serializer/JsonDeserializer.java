package serializer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class JsonDeserializer implements Deserializer<JsonNode> {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Logger logger = LoggerFactory.getLogger(JsonDeserializer.class);

    @Override
    public JsonNode deserialize(String topic, byte[] bytes) {
        JsonNode node = null;
        try {
            node = objectMapper.readTree(bytes);
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("Error deserializing", e);
        }
        return node;
    }
}
