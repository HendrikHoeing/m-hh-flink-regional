package flink.functions;

import java.util.Map;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import flink.kafka_utility.KafkaRecord;
import flink.utility.JsonGraphConverter;

public class CollectDataModels extends ProcessAllWindowFunction<Tuple2<String, Integer>, KafkaRecord, TimeWindow> {

    private static final long serialVersionUID = 1L;

    @Override
    public void process(ProcessAllWindowFunction<Tuple2<String, Integer>, KafkaRecord, TimeWindow>.Context context,
            Iterable<Tuple2<String, Integer>> elements, Collector<KafkaRecord> out) throws Exception {
        /**
         * Creates json graph object and kafka record for car model types and their
         * amount at given point
         */

        JsonObject key = new JsonObject();
        JsonObject data = new JsonObject();
        JsonObject jsonGraph = new JsonObject();
        JsonObject results = new JsonObject();

        JsonArray x = new JsonArray();
        JsonArray y = new JsonArray();

        for (Tuple2<String, Integer> value : elements) {
            if (!results.has(value.f0)) {
                results.addProperty(value.f0, value.f1);
            } else {
                results.addProperty(value.f0, results.get(value.f0).getAsInt() + value.f1);
            }
        }
        for (Map.Entry<String, JsonElement> entry : results.entrySet()) {
            x.add(entry.getKey());
            y.add(entry.getValue());
        }

        jsonGraph = JsonGraphConverter.convertGraph("Number of models", x, y, "Models", "Amount", "pie");

        data.add("jsonGraph", jsonGraph);
        data.add("results", results);

        key.addProperty("type", "models");

        out.collect(new KafkaRecord(key, data));

    }
}
