package flink;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic;

import flink.functions.*;
import flink.kafka_utility.*;
import flink.kafka_utility.KafkaRecord;

public class Main {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, // number of restart attempts
				10 // delay
		));
		env.setRestartStrategy(RestartStrategies.failureRateRestart(3, // max failures per interval
				org.apache.flink.api.common.time.Time.of(5, TimeUnit.MINUTES), // time interval for measuring failure
																				// rate
				org.apache.flink.api.common.time.Time.of(10, TimeUnit.SECONDS) // delay
		));

		// parse user parameters
		ParameterTool parameterTool = ParameterTool.fromArgs(args);
		String bootstrapServers = parameterTool.get("bootstrap-servers") != null
				? parameterTool.get("bootstrap-servers")
				: "kafka:9092,localhost:9093";
		String topicCarIn = parameterTool.get("topic-car-in") != null ? parameterTool.get("topic-car-in") : "car-eu";
		String topicCarOut = parameterTool.get("topic-car-out") != null ? parameterTool.get("topic-car-out")
				: "car-eu-analysis";
		String topicGlobal = parameterTool.get("topic-global") != null ? parameterTool.get("topic-global")
				: "region-eu-analysis";

		Properties propertiesConsumer = new Properties();
		propertiesConsumer.setProperty("bootstrap.servers", bootstrapServers); 
		propertiesConsumer.setProperty("group.id", "flink");

		// Consumer
		FlinkKafkaConsumer<KafkaRecord> kafkaConsumer = new FlinkKafkaConsumer<KafkaRecord>(topicCarIn, new KafkaDeserialization(), propertiesConsumer);
		kafkaConsumer.setStartFromEarliest();

		// Producer
		FlinkKafkaProducer<KafkaRecord> kafkaProducerRegion = new FlinkKafkaProducer<KafkaRecord>(topicGlobal,
				new KafkaSerialization(topicGlobal), propertiesConsumer, Semantic.EXACTLY_ONCE);
		FlinkKafkaProducer<KafkaRecord> kafkaProducerCar = new FlinkKafkaProducer<KafkaRecord>(topicCarOut,
				new KafkaSerialization(topicCarOut), propertiesConsumer, Semantic.EXACTLY_ONCE);

		DataStream<KafkaRecord> regionStream = env.addSource(kafkaConsumer).name("Car Stream");

		KeyedStream<KafkaRecord, String> carStream = regionStream.filter(record -> record != null)
				.keyBy(record -> record.data.get("id").getAsString());

		// Detect cars with high speed
		carStream.window(TumblingProcessingTimeWindows.of(Time.seconds(3))).aggregate(new HighSpeedDetector())
				.filter(record -> record != null).addSink(kafkaProducerCar).name("High Speed Detector");
		;

		// Counts all active cars
		regionStream.filter(record -> record != null).windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
				.aggregate(new ActiveCarsDetector()).addSink(kafkaProducerRegion).name("Active Cars Detector");
		;

		/*
		 * Count distinct car models
		 * 
		 * Aggregate all distinct IDs into one Tuple ("model", 1). Collect data and
		 * produce kafka record
		 */
		carStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5))) // Every 5 seconds
				.aggregate(new ModelTypeDetector()).windowAll(TumblingProcessingTimeWindows.of(Time.seconds(1)))
				.process(new CollectDataModels()).addSink(kafkaProducerRegion).name("Model Types Detector");
		;

		/*
		 * Count distinct fuel types
		 * 
		 * Aggregate all distinct IDs into one Tuple ("fuel", 1). Collect data and
		 * produce kafka record
		 */
		carStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5))).aggregate(new FuelTypeDetector())
				.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(1))).process(new CollectDataFuel())
				.addSink(kafkaProducerRegion).name("Fuel Types Detector");
		;

		/*
		 * Position of all cars
		 * 
		 * Returns position of latest record in timeframe Collects positions and
		 * produces output record
		 */
		carStream.window(TumblingProcessingTimeWindows.of(Time.seconds(1))).process(new PosProcesser())
				.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(1))).process(new CollectDataPos())
				.addSink(kafkaProducerRegion).name("Position Collector");

		System.out.println("Flink Job started.");

		env.execute();

	}
}
