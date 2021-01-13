package flink.functions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.gson.JsonObject;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import flink.kafka_utility.KafkaRecord;

public class HighSpeedDetector implements AggregateFunction<KafkaRecord, List<KafkaRecord>, KafkaRecord> {

    private static final long serialVersionUID = 1L;

    private static final double HIGH_SPEED = 100.00;

    @Override
    public List<KafkaRecord> createAccumulator() {
        return new ArrayList<KafkaRecord>();

    }

    @Override
    public List<KafkaRecord> add(KafkaRecord record, List<KafkaRecord> accumulator) {
        accumulator.add(record);
        return accumulator;
    }

    @Override
    public KafkaRecord getResult(List<KafkaRecord> accumulator) {
        JsonObject data = new JsonObject();
        String key = accumulator.get(0).key;
        Double mphTotal = 0.0;
        int numRecords = 0;

        // Look through all records
        for (KafkaRecord record : accumulator) {
            mphTotal += record.data.get("mph").getAsDouble();
            numRecords++;
        }

        if ((mphTotal / numRecords) > HIGH_SPEED) {
            data.addProperty("info", "Speed too high!");
            return new KafkaRecord(key, data, "car-usa-info");
        } else {
            return null;
        }
    }

    @Override
    public List<KafkaRecord> merge(List<KafkaRecord> a, List<KafkaRecord> b) {
        a.addAll(b);
        return a;
    }

}
