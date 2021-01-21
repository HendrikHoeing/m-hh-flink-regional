package flink.functions;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import flink.kafka_utility.KafkaRecord;
import flink.utility.JsonGraphConverter;

public class CollectDataPos extends ProcessAllWindowFunction<Tuple2<Float, Float>, KafkaRecord, TimeWindow> {

    private static final Long serialVersionUID = 1L;

    @Override
    public void process(ProcessAllWindowFunction<Tuple2<Float, Float>, KafkaRecord, TimeWindow>.Context context,
            Iterable<Tuple2<Float, Float>> elements, Collector<KafkaRecord> out) throws Exception {
        /**
         * Creates json graph object and kafka record for distinct car positions at
         * given point
         */

        JsonObject key = new JsonObject();
        JsonObject data = new JsonObject();
        JsonObject jsonGraph = new JsonObject();
        JsonObject results = new JsonObject();

        JsonArray x = new JsonArray();
        JsonArray y = new JsonArray();

        for (Tuple2<Float, Float> value : elements) {
            x.add(value.f0);
            y.add(value.f1);
        }

        jsonGraph = JsonGraphConverter.convertGraph("Position of cars in the usa", x, y, "", "", "scattergeo");

        JsonObject scope = new JsonObject();
        scope.addProperty("scope", "usa");
        jsonGraph.add("geo", scope);

        data.add("jsonGraph", jsonGraph);
        data.add("results", results);

        key.addProperty("type", "pos");

        out.collect(new KafkaRecord(key, data));
    }
}
