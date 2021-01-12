package flink.functions;

import com.google.gson.JsonObject;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import flink.kafka_utility.KafkaRecord;

public class CollectDataFuel extends ProcessAllWindowFunction<Tuple2<String, Integer>, KafkaRecord, TimeWindow> {

    private static final long serialVersionUID = 1L;

    @Override
    public void process(ProcessAllWindowFunction<Tuple2<String, Integer>, KafkaRecord, TimeWindow>.Context context,
            Iterable<Tuple2<String, Integer>> elements, Collector<KafkaRecord> out) throws Exception {

        JsonObject data = new JsonObject();
        JsonObject head = new JsonObject();
        
        for (Tuple2<String, Integer> value : elements) {

            if (!data.has(value.f0)) {
                data.addProperty(value.f0, value.f1);
            } else {
                data.addProperty(value.f0, data.get(value.f0).getAsInt() + value.f1);
            }
        }

        head.add("fuel", data);

        out.collect(new KafkaRecord(head));
    }
}
