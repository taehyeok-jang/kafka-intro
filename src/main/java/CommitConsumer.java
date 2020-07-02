import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

@Slf4j
public class CommitConsumer {

    private static final String HOST = "localhost:9092";

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", HOST);
        props.put("group.id", "g1");
        props.put("key.deserializer", KafkaStringSerDes.DIR_DES);
        props.put("value.deserializer", KafkaStringSerDes.DIR_DES);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

        consumer.subscribe(Collections.singletonList("my-topic"), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                log.debug("Lost partitions in rebalance. Commiting current offsets: " + currentOffsets);
                consumer.commitSync(currentOffsets);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

            }
        });

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    log.debug("topic = %s, partition: %s, offset = %d," +
                                    "key = %s, country = %s\n",
                            record.topic(), record.partition(), record.offset(),
                            record.key(), record.value());

                    currentOffsets.put(
                            new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1, "no metadata")
                    );
                    consumer.commitAsync(currentOffsets, null);
                }


                consumer.commitAsync();
            }
        } catch (Exception e) {
          log.error("Unexpected error", e);
        } finally {
            try {
                consumer.commitSync();
            } finally {
                consumer.close();
            }
        }
    }
}
