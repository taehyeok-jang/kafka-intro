public class KafkaStringSerDes {

    public static final String DIR_SER = "org.apache.kafka.common.serialization.StringSerializer";
    public static final String DIR_DES = "org.apache.kafka.common.serialization.StringDeserializer";

    public static String serialize(Integer input) {
        return Integer.toString(input);
    }
}
