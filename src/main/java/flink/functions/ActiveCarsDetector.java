package flink.functions;

import java.util.ArrayList;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.flink.api.common.functions.AggregateFunction;

import flink.kafka_utility.KafkaRecord;
import flink.utility.JsonGraphConverter;

public class ActiveCarsDetector implements AggregateFunction<KafkaRecord, ArrayList<KafkaRecord>, KafkaRecord> {

    private static final long serialVersionUID = 1L;

    @Override
    public ArrayList<KafkaRecord> createAccumulator() {
        return new ArrayList<KafkaRecord>();
    }

    @Override
    public ArrayList<KafkaRecord> add(KafkaRecord record, ArrayList<KafkaRecord> accumulator) {
        accumulator.add(record);
        return accumulator;
    }

    @Override
    public KafkaRecord getResult(ArrayList<KafkaRecord> accumulator) {
        
        JsonObject key = new JsonObject();
        JsonObject data = new JsonObject();

        JsonArray x = new JsonArray();
        JsonArray y = new JsonArray();

        ArrayList<String> distinctCars = new ArrayList<>();

        for (KafkaRecord record : accumulator) {
            if (!distinctCars.contains(record.key.get("id").getAsString())) {
                distinctCars.add(record.key.get("id").getAsString());
            }
        }

        data = JsonGraphConverter.convertGraph("Number of active cars", "Time", "Amount", "scatter", null);

        data.addProperty("x", System.currentTimeMillis());
        data.addProperty("y", distinctCars.size());
 
        key.addProperty("region", "usa");
        key.addProperty("type", "numActiveCars");

        return new KafkaRecord(key, data, "region-usa-info");
    }

    @Override
    public ArrayList<KafkaRecord> merge(ArrayList<KafkaRecord> a, ArrayList<KafkaRecord> b) {
        a.addAll(b);
        return a;
    }

}
