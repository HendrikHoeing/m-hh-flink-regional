package flink.functions;

import java.util.ArrayList;
import com.google.gson.JsonObject;
import org.apache.flink.api.common.functions.AggregateFunction;

import flink.kafka_utility.KafkaRecord;

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
        JsonObject data = new JsonObject();
        JsonObject key = new JsonObject();

        ArrayList<String> distinctCars = new ArrayList<>();

        for (KafkaRecord record : accumulator) {
            if (!distinctCars.contains(record.key.get("id").getAsString())) {
                distinctCars.add(record.key.get("id").getAsString());
            }
        }

        data.addProperty("x", System.currentTimeMillis());
        data.addProperty("y", distinctCars.size());
        data.addProperty("xname", "");
        data.addProperty("yname", "Amount");
        data.addProperty("type", "scatter");
        
        key.addProperty("type", "numActiveCars");

        return new KafkaRecord(key, data, "region-usa-info");
    }

    @Override
    public ArrayList<KafkaRecord> merge(ArrayList<KafkaRecord> a, ArrayList<KafkaRecord> b) {
        a.addAll(b);
        return a;
    }

}
