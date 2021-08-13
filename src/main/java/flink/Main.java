package flink;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import flink.functions.*;
import flink.kafka_utility.*;
import flink.kafka_utility.KafkaRecord;
import flink.utility.ConfigLoader;

public class Main {

	public static void main(String[] args) throws Exception {

		Properties kafkaProperties = null;

        try {
            kafkaProperties = (new ConfigLoader()).getProperties("kafka.properties");
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }

		String topicCarIn = kafkaProperties.getProperty("topic-car-in");
		String topicCarOut = kafkaProperties.getProperty("topic-car-out");
		String topicFilterOut = kafkaProperties.getProperty("topic-filter-out");
		String region = kafkaProperties.getProperty("region");

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, // number of restart attempts
				10 // delay
		));
		env.setRestartStrategy(RestartStrategies.failureRateRestart(3, // max failures per interval
				org.apache.flink.api.common.time.Time.of(5, TimeUnit.MINUTES), // time interval for measuring failure
																				// rate
				org.apache.flink.api.common.time.Time.of(10, TimeUnit.SECONDS) // delay
		));


		// Consumer
		FlinkKafkaConsumer<KafkaRecord> kafkaConsumer = new FlinkKafkaConsumer<KafkaRecord>(topicCarIn,
				new KafkaDeserialization(), kafkaProperties);
		kafkaConsumer.setStartFromGroupOffsets();
		FlinkKafkaConsumer<String> kafkaConsumerRaw = new FlinkKafkaConsumer<String>(topicCarIn,
				new KafkaDeserializationString(), kafkaProperties);
		kafkaConsumerRaw.setStartFromGroupOffsets();

		// Producer
		FlinkKafkaProducer<KafkaRecord> kafkaProducerCar = new FlinkKafkaProducer<KafkaRecord>(topicCarOut,
				new KafkaSerialization(topicCarOut), kafkaProperties, Semantic.EXACTLY_ONCE);
		FlinkKafkaProducer<KafkaRecord> kafkaProducerFilter = new FlinkKafkaProducer<KafkaRecord>(topicFilterOut,
				new KafkaSerialization(topicFilterOut), kafkaProperties, Semantic.EXACTLY_ONCE);

		DataStream<KafkaRecord> regionStream = env.addSource(kafkaConsumer).name("Car Stream");

		// Decodes kafka message to string and persists it to the data lake
		final StreamingFileSink<String> dataLake = StreamingFileSink
				.forRowFormat(new Path("C:\\Masterarbeit\\data\\eu\\raw\\" + topicCarIn), //"hdfs://namenode:9000/flink/"
						new SimpleStringEncoder<String>("UTF-8"))
						.withRollingPolicy(
							DefaultRollingPolicy.builder()
								.withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
								.withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
								.withMaxPartSize(1024 * 1024 * 1024)
								.build())
				.build();
		env.addSource(kafkaConsumerRaw).addSink(dataLake).name("Raw stream to data lake");

		// Filter values for global topic: consumption, co2, geochip, wearing parts
		regionStream.filter(record -> record != null).process(new
		FilterProcessor(region)).addSink(kafkaProducerFilter);

		// Health status person
		regionStream.filter(record -> record != null).process(new
		HealthProcessor()).addSink(kafkaProducerCar);

		// Recommendations
		regionStream.filter(record -> record != null).process(new
		RecommendationProcessor()).addSink(kafkaProducerCar);

		System.out.println("Flink Job started.");

		env.execute();

	}

}
