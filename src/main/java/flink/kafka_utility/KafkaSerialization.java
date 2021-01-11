package flink.kafka_utility;

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaSerialization 
  implements KafkaSerializationSchema<KafkaRecord> {

    private static final long serialVersionUID = 1L;

    @Override
    public ProducerRecord<byte[], byte[]> serialize(KafkaRecord record, Long timestamp) {
        return new ProducerRecord<byte[],byte[]>("car-usa-info", record.key.getBytes(), record.value.getBytes()); //0, timestamp, 
    }

}
