package flink;

import java.util.Map;

public class KafkaRecord {
	String key;
	Map<String,Object> value;
	long timestamp;
	long offset;
	int partition;
	String topic;
}
