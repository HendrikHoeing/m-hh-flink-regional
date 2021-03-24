package flink.kafka_utility;

import java.nio.charset.StandardCharsets;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class KafkaDeserializationString implements KafkaDeserializationSchema<String> {

	private static final long serialVersionUID = 2L;

	@Override
	public TypeInformation<String> getProducedType() {
		return TypeInformation.of(String.class);
	}

	@Override
	public String deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {

		Gson gson = new Gson();

		try {
			JsonObject message = new JsonObject();

			message.addProperty("timestamp", record.timestamp());
			message.addProperty("topic", record.topic());
			message.addProperty("offset", record.offset());
			if (record.key() != null) {
				message.addProperty("key", new String(record.key(), StandardCharsets.UTF_8));
			}
			message.addProperty("value", new String(record.value(), StandardCharsets.UTF_8));

			return gson.toJson(message);
		} catch (Exception e) {
			System.out.println("Error deserializing kafka record: " + e.getMessage());
			System.out.println("Continuing with next record...");
			return null;
		}
	}

	@Override
	public boolean isEndOfStream(String record) {
		return false;
	}
}
