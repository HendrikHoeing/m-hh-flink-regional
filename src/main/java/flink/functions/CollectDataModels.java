package flink.functions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import flink.kafka_utility.KafkaRecord;

public class CollectDataModels extends ProcessAllWindowFunction<Tuple2<String, Integer>, KafkaRecord, TimeWindow> {

    private static final long serialVersionUID = 1L;

    @Override
    public void process(ProcessAllWindowFunction<Tuple2<String, Integer>, KafkaRecord, TimeWindow>.Context context,
            Iterable<Tuple2<String, Integer>> elements, Collector<KafkaRecord> out) throws Exception {

        JsonObject key = new JsonObject();
        JsonObject data = new JsonObject();

        JsonObject data_distinct = new JsonObject();
        JsonArray x = new JsonArray();
        JsonArray y = new JsonArray();

        key.addProperty("type", "models");

        for (Tuple2<String, Integer> value : elements) {
            if (!data_distinct.has(value.f0)) {
                data_distinct.addProperty(value.f0, value.f1);
            } else {
                data_distinct.addProperty(value.f0, data_distinct.get(value.f0).getAsInt() + value.f1);
            }
        }
        for (Map.Entry<String, JsonElement> entry : data_distinct.entrySet()) {
            x.add(entry.getKey());
            y.add(entry.getValue());
        }

        data.add("x", x);
        data.add("y", y);
        data.addProperty("xname", "Model");
        data.addProperty("yname", "Amount");
        data.addProperty("type", "scatter");

        out.collect(new KafkaRecord(key, data, "region-usa-info"));

    }
}
