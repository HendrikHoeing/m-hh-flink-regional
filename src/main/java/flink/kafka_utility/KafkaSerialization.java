package flink.kafka_utility;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaSerialization implements KafkaSerializationSchema<KafkaRecord> {

    private static final long serialVersionUID = 1L;

    @Override
    public ProducerRecord<byte[], byte[]> serialize(KafkaRecord record, Long timestamp) {

        Gson gson = new Gson();

        System.out.println("Topic: region-usa-info || Key: " + record.key + " || Value: " + record.data);

        if (record.key != null) {
            return new ProducerRecord<byte[], byte[]>("region-usa-info", record.key.getBytes(),
                    gson.toJson(record.data).getBytes());
        } else {
            return new ProducerRecord<byte[], byte[]>("region-usa-info", gson.toJson(record.data).getBytes());
        }
    }

}
