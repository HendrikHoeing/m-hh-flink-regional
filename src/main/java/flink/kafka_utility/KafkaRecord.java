package flink.kafka_utility;

import java.util.Map;

public class KafkaRecord {

	public KafkaRecord() {

	};

	public KafkaRecord(String key, String value) {
		this.key = key;
		this.value = value;
	}

	public String key;
	public Map<String, Object> data;
	public String value;
	public long timestamp;
	public long offset;
	public int partition;
	public String topic;
}
