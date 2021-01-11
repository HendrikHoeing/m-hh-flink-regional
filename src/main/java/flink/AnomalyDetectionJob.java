package flink;

import java.util.Properties;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic;


import flink.functions.HighSpeedDetector;
import flink.kafka_utility.KafkaRecord;
import flink.kafka_utility.KafkaSerialization;
import flink.kafka_utility.KafkaDeserialization;


public class AnomalyDetectionJob {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// parse user parameters
		// ParameterTool parameterTool = ParameterTool.fromArgs(args);

		Properties propertiesConsumer = new Properties();
		propertiesConsumer.setProperty("bootstrap.servers", "localhost:9092");
		propertiesConsumer.setProperty("group.id", "car");

		// Consumer
		FlinkKafkaConsumer<KafkaRecord> kafkaConsumer = new FlinkKafkaConsumer<KafkaRecord>("car-usa",
				new KafkaDeserialization(), propertiesConsumer);

		// start from the latest record
		kafkaConsumer.setStartFromLatest();

		// Producer
		FlinkKafkaProducer<KafkaRecord> kafkaProducer = new FlinkKafkaProducer<KafkaRecord>("car-usa-info",
				new KafkaSerialization(), propertiesConsumer, Semantic.EXACTLY_ONCE);

		DataStream<KafkaRecord> carStream = env.addSource(kafkaConsumer);

		// Time window function
		carStream.keyBy(record -> record.key) // High costs
				.window(TumblingProcessingTimeWindows.of(Time.seconds(5))).aggregate(new HighSpeedDetector()).addSink(kafkaProducer);

		env.execute();

	}
}
