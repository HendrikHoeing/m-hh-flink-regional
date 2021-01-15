package flink.functions;

import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import flink.kafka_utility.KafkaRecord;
import flink.utility.JsonGraphConverter;

public class CollectDataFuel extends ProcessAllWindowFunction<Tuple2<String, Integer>, KafkaRecord, TimeWindow> {

    private static final long serialVersionUID = 1L;

    @Override
    public void process(ProcessAllWindowFunction<Tuple2<String, Integer>, KafkaRecord, TimeWindow>.Context context,
            Iterable<Tuple2<String, Integer>> elements, Collector<KafkaRecord> out) throws Exception {

        JsonObject key = new JsonObject();
        JsonObject data = new JsonObject();

        JsonArray x = new JsonArray();
        JsonArray y = new JsonArray();

        for (Tuple2<String, Integer> value : elements) {

            if (!data.has(value.f0)) {
                data.addProperty(value.f0, value.f1);
            } else {
                data.addProperty(value.f0, data.get(value.f0).getAsInt() + value.f1);
            }
        }

        for (Map.Entry<String, JsonElement> entry : data.entrySet()) {
            x.add(entry.getKey());
            y.add(entry.getValue());
        }

        data = JsonGraphConverter.convertGraph("Number of fuel types", x, y, "Fuel Type", "Amount", "scatter", null);

        key.addProperty("region", "usa");
        key.addProperty("type", "fuel");

        out.collect(new KafkaRecord(key, data, "region-usa-info"));
    }
}
