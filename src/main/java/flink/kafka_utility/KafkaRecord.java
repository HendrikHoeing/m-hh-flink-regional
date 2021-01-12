package flink.kafka_utility;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonObject;

import org.apache.flink.api.java.tuple.Tuple2;

public class KafkaRecord {

	public KafkaRecord() {

	};

	public KafkaRecord(JsonObject data) {
		this.data = data;
	};

	public KafkaRecord(String key, JsonObject data) {
		this.key = key;
		this.data = data;
	}

	public String key;
	public JsonObject data = new JsonObject();
	public long timestamp;
	public long offset;
	public int partition;
	public String topic;
}
