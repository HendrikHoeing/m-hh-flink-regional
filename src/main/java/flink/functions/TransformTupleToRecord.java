package flink.functions;
import com.google.gson.JsonObject;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import flink.kafka_utility.KafkaRecord;

public class TransformTupleToRecord extends ProcessFunction<Tuple2<String, Integer>, KafkaRecord> {

    private static final long serialVersionUID = 1L;

    @Override
    public void processElement(Tuple2<String, Integer> value,
            ProcessFunction<Tuple2<String, Integer>, KafkaRecord>.Context ctx, Collector<KafkaRecord> out)
            throws Exception {

        JsonObject data = new JsonObject();
        data.addProperty(value.f0, value.f1);

        out.collect(new KafkaRecord(data));
    }

}
