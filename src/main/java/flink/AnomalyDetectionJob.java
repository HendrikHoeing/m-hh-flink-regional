package flink;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.google.gson.Gson;


public class AnomalyDetectionJob {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// parse user parameters
		// ParameterTool parameterTool = ParameterTool.fromArgs(args);

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("group.id", "car");
		
		
		// Consumer
		FlinkKafkaConsumer<KafkaRecord> kafkaConsumer = new FlinkKafkaConsumer<KafkaRecord>("car-usa", new RecordDeserialization(),
				properties);

		// start from the latest record
		kafkaConsumer.setStartFromLatest(); 

		// Producer
		//FlinkKafkaProducer kafkaProducer = new FlinkKafkaProducer<KafkaRecord>("car-usa-info", (SerializationSchema<KafkaRecord>) new RecordDeserialization(), properties);
		// ,Semantic.EXACTLY_ONCE
		
		DataStream<KafkaRecord> carStream = env.addSource(kafkaConsumer);
		
		
		//Keyed processed function
		carStream
		.keyBy(record -> record.key) //High costs
		.process(new AnomalyDetector())
		.print(); //Console as sink
		
		
		
		//Time window function
		// carStream
		// .filter((record) -> record.value != null)
		// .keyBy(record -> record.key) //High costs
		// .window(TumblingEventTimeWindows.of(Time.milliseconds(5)))
        // .process(new WindowAnomalyDetector())
        // .print();
		
		 
		
		//carStream.addSink(kafkaProducer).name("send-info");

		env.execute();

	}
}

class RecordDeserialization implements KafkaDeserializationSchema<KafkaRecord> {

	private static final long serialVersionUID = 2L;

	@Override
	public TypeInformation<KafkaRecord> getProducedType() {
		// TODO Auto-generated method stub
		return TypeInformation.of(KafkaRecord.class);
	}

	@Override
	public KafkaRecord deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
		// TODO Auto-generated method stub
		KafkaRecord rec = new KafkaRecord();
		
		Gson gson = new Gson();
		
		Map metadata = gson.fromJson(new String(record.key(), StandardCharsets.UTF_8), Map.class);
		rec.key = (String) metadata.get("carId");
		rec.value = gson.fromJson(new String(record.value(), StandardCharsets.UTF_8), Map.class);
		
		rec.timestamp = record.timestamp();
		rec.topic = record.topic();
		rec.partition = record.partition();
		rec.offset = record.offset();

		return rec;
	}

	@Override
	public boolean isEndOfStream(KafkaRecord record) {
		// TODO Auto-generated method stub
		return false;
	}
}
