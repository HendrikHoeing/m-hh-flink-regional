package flink;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic;
import org.apache.kafka.clients.producer.ProducerRecord;

// import org.springframework.boot.SpringApplication;
// import org.springframework.boot.autoconfigure.SpringBootApplication;
// import org.springframework.cloud.netflix.eureka.EnableEurekaClient;

import flink.functions.*;
import flink.kafka_utility.*;
import flink.kafka_utility.KafkaRecord;

// @SpringBootApplication
// @EnableEurekaClient
public class Main {

	public static void main(String[] args) throws Exception {

		// Register with Eureka registry
		// SpringApplication.run(Main.class, args);

		// TODO https://graphql.org/code/#java-kotlin -> Liste von Werkstätten + Slots,
		// Tankstellen + Preise

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
		String region = parameterTool.get("region") != null ? parameterTool.get("region") : "eu";
		String bootstrapServers = parameterTool.get("bootstrap-servers") != null
				? parameterTool.get("bootstrap-servers")
				: "localhost:9093";
		String topicCarIn = parameterTool.get("topic-car-in") != null ? parameterTool.get("topic-car-in") : "car-eu";
		String topicCarOut = parameterTool.get("topic-car-out") != null ? parameterTool.get("topic-car-out")
				: "car-eu-analysis";
		String topicRegionalAnalysis = parameterTool.get("topic-region-analysis") != null
				? parameterTool.get("topic-region-analysis")
				: "region-eu-analysis";
		String topicGlobalFilter = parameterTool.get("topic-region-filter") != null
				? parameterTool.get("topic-region-filter")
				: "region-eu-filter";

		Properties propertiesConsumer = new Properties();
		propertiesConsumer.setProperty("bootstrap.servers", bootstrapServers);
		propertiesConsumer.setProperty("group.id", "flink");

		// Consumer
		FlinkKafkaConsumer<KafkaRecord> kafkaConsumer = new FlinkKafkaConsumer<KafkaRecord>(topicCarIn,
				new KafkaDeserialization(), propertiesConsumer);
		kafkaConsumer.setStartFromLatest();
		FlinkKafkaConsumer<String> kafkaConsumerRaw = new FlinkKafkaConsumer<String>(topicCarIn,
				new KafkaDeserializationString(), propertiesConsumer);
		kafkaConsumerRaw.setStartFromLatest();

		// Producer
		FlinkKafkaProducer<KafkaRecord> kafkaProducerCar = new FlinkKafkaProducer<KafkaRecord>(topicCarOut,
				new KafkaSerialization(topicCarOut), propertiesConsumer, Semantic.EXACTLY_ONCE);
		FlinkKafkaProducer<KafkaRecord> kafkaProducerFilter = new FlinkKafkaProducer<KafkaRecord>(topicGlobalFilter,
				new KafkaSerialization(topicGlobalFilter), propertiesConsumer, Semantic.EXACTLY_ONCE);

		DataStream<KafkaRecord> regionStream = env.addSource(kafkaConsumer).name("Car Stream");

		// Decodes kafka message to string and persists it to the data lake
		final StreamingFileSink<String> dataLake = StreamingFileSink
				.forRowFormat(new Path("hdfs://localhost:9000/flink/" + topicCarIn),
						new SimpleStringEncoder<String>("UTF-8"))
				.build();
		env.addSource(kafkaConsumerRaw).print();
		env.addSource(kafkaConsumerRaw).addSink(dataLake).name("Raw stream to data lake");

		// // Filter values for global topic: consumption, co2, geochip, wearing parts
		// regionStream.filter(record -> record != null).process(new
		// FilterProcessor(region)).addSink(kafkaProducerFilter);

		// // Health status person
		// regionStream.filter(record -> record != null).process(new
		// HealthProcessor()).addSink(kafkaProducerCar);

		// // Recommendations
		// regionStream.filter(record -> record != null).process(new
		// RecommendationProcessor()).addSink(kafkaProducerCar);

		System.out.println("Flink Job started.");

		env.execute();

	}

}
