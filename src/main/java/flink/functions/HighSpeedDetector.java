package flink.functions;

import java.util.*;
import com.google.gson.JsonObject;

import org.apache.flink.api.common.functions.AggregateFunction;

import flink.kafka_utility.KafkaRecord;

public class HighSpeedDetector implements AggregateFunction<KafkaRecord, List<KafkaRecord>, KafkaRecord> {

    private static final long serialVersionUID = 1L;

    private static final double HIGH_SPEED = 150.00;

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
        /**
         * Creates kafka record for if speed limit above given threshhold
         */

        JsonObject data = new JsonObject();
        JsonObject key = accumulator.get(0).key;
        Double kmhTotal = 0.0;
        int numRecords = 0;

        // Look through all records
        for (KafkaRecord record : accumulator) {
            kmhTotal += record.data.get("kmh").getAsDouble();
            numRecords++;
        }

        if ((kmhTotal / numRecords) > HIGH_SPEED) {
            data.addProperty("info", "Speed too high!");
            return new KafkaRecord(key, data);
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
