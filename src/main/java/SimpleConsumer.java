import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
public class SimpleConsumer {

    private static final String HOST = "localhost:9092";

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", HOST);
        props.put("group.id", "g1");
        props.put("key.deserializer", KafkaStringSerDes.DIR_DES);
        props.put("value.deserializer", KafkaStringSerDes.DIR_DES);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        consumer.subscribe(Collections.singletonList("my-topic"));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    log.debug("topic = %s, partition: %s, offset = %d," +
                                    "key = %s, country = %s\n",
                            record.topic(), record.partition(), record.offset(),
                            record.key(), record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }
}
