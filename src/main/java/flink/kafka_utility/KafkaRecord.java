package flink.kafka_utility;

import com.google.gson.JsonObject;


public class KafkaRecord {

	public KafkaRecord() {

	};

	public KafkaRecord(JsonObject data) {
		this.data = data;
	};

	public KafkaRecord(JsonObject key, JsonObject data) {
		this.key = key;
		this.data = data;
	}

	public JsonObject key = new JsonObject();
	public JsonObject data = new JsonObject();
	public long timestamp;
	public long offset;
	public int partition;
	public String topic;
}
