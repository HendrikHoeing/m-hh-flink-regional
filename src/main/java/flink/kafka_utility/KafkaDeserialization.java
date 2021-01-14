package flink.kafka_utility;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class KafkaDeserialization implements KafkaDeserializationSchema<KafkaRecord> {

	private static final long serialVersionUID = 2L;

	@Override
	public TypeInformation<KafkaRecord> getProducedType() {
		return TypeInformation.of(KafkaRecord.class);
	}

	@Override
	public KafkaRecord deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {

		KafkaRecord rec = new KafkaRecord();

		Gson gson = new Gson();

		Map<String, Object> metadata = gson.fromJson(new String(record.key(), StandardCharsets.UTF_8), Map.class);

		rec.key.addProperty("carId", (String) metadata.get("carId"));

		// Decode and cast to JSON Object
		rec.data = gson.fromJson(new String(record.value(), StandardCharsets.UTF_8), JsonObject.class);

		rec.timestamp = record.timestamp();
		rec.topic = record.topic();
		rec.partition = record.partition();
		rec.offset = record.offset();

		return rec;
	}

	@Override
	public boolean isEndOfStream(KafkaRecord record) {
		return false;
	}
}
