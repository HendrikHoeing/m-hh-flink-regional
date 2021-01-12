package flink;

import java.util.List;
import java.util.Properties;

import com.google.gson.JsonObject;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic;
import org.apache.flink.util.Collector;

import flink.functions.ActiveCarsDetector;
import flink.functions.CollectDataFuel;
import flink.functions.CollectDataModels;
import flink.functions.FuelTypeDetector;
import flink.functions.HighSpeedDetector;
import flink.functions.ModelTypeDetector;
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
		kafkaConsumer.setStartFromLatest();

		// Producer
		FlinkKafkaProducer<KafkaRecord> kafkaProducer = new FlinkKafkaProducer<KafkaRecord>("car-usa-info",
				new KafkaSerialization(), propertiesConsumer, Semantic.EXACTLY_ONCE);

		DataStream<KafkaRecord> regionStream = env.addSource(kafkaConsumer);

		KeyedStream<KafkaRecord, String> carStream = regionStream.keyBy(record -> record.key); // keyBy -> High costs

		/// Functions

		// Detect cars with high speed
		carStream.window(TumblingProcessingTimeWindows.of(Time.seconds(3))).aggregate(new HighSpeedDetector())
				.filter(record -> record != null).addSink(kafkaProducer);

		// Counts all active cars every x seconds
		regionStream.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5))).aggregate(new ActiveCarsDetector())
				.addSink(kafkaProducer);

		// Count all distinct car models
		carStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5))) // Every 5 seconds
				.aggregate(new ModelTypeDetector())// Aggregate all distinct IDs into one Tuple (model, 1)
				// Collect data from all windows and transform to one kafka record
				.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(1))).process(new CollectDataModels())
				.addSink(kafkaProducer);

		// Count all distinct fuel types
		carStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5))) // Every 5 seconds
				.aggregate(new FuelTypeDetector()) // Aggregate all distinct IDs into one Tuple (fuel, 1)
				// Collect data from all windows and transform to one kafka record
				.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(1))).process(new CollectDataFuel())
				.addSink(kafkaProducer);

		env.execute();

	}
}
