import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SimpleProducer {

    private static final String HOST = "localhost:9092";

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Properties props = new Properties();
        props.put("bootstrap.servers", HOST);
        props.put("acks", "all");
        props.put("key.serializer", KafkaStringSerDes.DIR_SER);
        props.put("value.serializer", KafkaStringSerDes.DIR_SER);

        Producer<String, String> producer = new KafkaProducer<>(props);

        try {
            for (int i = 0; i < 20; i++) {
                ProducerRecord record = new ProducerRecord<>("my-topic", KafkaStringSerDes.serialize(i), KafkaStringSerDes.serialize(i));
                producer.send(record); // fire-and-forget
                producer.send(record).get(); // synchronous.
                producer.send(record, (metadata, e) -> { // asynchronous with callback.
                    System.out.println("offset: " + metadata.offset());
                    System.out.println("partition: " + metadata.partition());
                    System.out.println("topic: " + metadata.topic() );
                    if (e != null) {
                        e.printStackTrace();
                    }
                });
            }
        } finally {
            producer.close();
        }
    }
}
