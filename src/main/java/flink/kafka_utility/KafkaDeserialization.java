package flink.kafka_utility;

import java.nio.charset.StandardCharsets;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.flink.api.common.typeinfo.TypeInformation;
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

		try {
			// Decode and cast to JSON Object
			rec.data = gson.fromJson(new String(record.value(), StandardCharsets.UTF_8), JsonObject.class);

			rec.key.addProperty("id", rec.data.get("id").getAsString());

			rec.timestamp = record.timestamp();
			rec.topic = record.topic();
			rec.partition = record.partition();
			rec.offset = record.offset();

			return rec;
		} catch (Exception e) {
			System.out.println("Error deserializing kafka record: " + e.getMessage());
			System.out.println("Continuing with next record...");
			return null;
		}
	}

	@Override
	public boolean isEndOfStream(KafkaRecord record) {
		return false;
	}
}
