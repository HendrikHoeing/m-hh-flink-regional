package flink.kafka_utility;

import com.google.gson.Gson;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaSerialization implements KafkaSerializationSchema<KafkaRecord> {

    private static final long serialVersionUID = 1L;
    private String topic;

    public KafkaSerialization(String topic){
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(KafkaRecord record, Long timestamp) {

        Gson gson = new Gson();

        if (record.key != null) {
            return new ProducerRecord<byte[], byte[]>(this.topic, gson.toJson(record.key).getBytes(),
                    gson.toJson(record.data).getBytes());
        } else {
            return new ProducerRecord<byte[], byte[]>(this.topic, gson.toJson(record.data).getBytes());
        }
    }

}
