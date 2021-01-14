package flink.kafka_utility;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonObject;

import org.apache.flink.api.java.tuple.Tuple2;

public class KafkaRecord {

	public KafkaRecord() {

	};

	public KafkaRecord(JsonObject data, String topic) {
		this.data = data;
		this.topic = topic;
	};

	public KafkaRecord(JsonObject key, JsonObject data, String topic) {
		this.key = key;
		this.data = data;
		this.topic = topic;
	}

	public JsonObject key = new JsonObject();
	public JsonObject data = new JsonObject();
	public long timestamp;
	public long offset;
	public int partition;
	public String topic;
}
