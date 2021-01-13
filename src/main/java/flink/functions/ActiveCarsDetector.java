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
        ArrayList<String> distinctCars = new ArrayList<>();

        for (KafkaRecord record : accumulator) {
            if (!distinctCars.contains(record.key)) {
                distinctCars.add(record.key);
            }
        }

        data.addProperty("numActiveCars", distinctCars.size());
        return new KafkaRecord(data, "region-usa-info");
    }

    @Override
    public ArrayList<KafkaRecord> merge(ArrayList<KafkaRecord> a, ArrayList<KafkaRecord> b) {
        a.addAll(b);
        return a;
    }

}
