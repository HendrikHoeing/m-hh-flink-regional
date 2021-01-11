package flink.functions;

import java.util.ArrayList;

import org.apache.flink.api.common.functions.AggregateFunction;

import flink.kafka_utility.KafkaRecord;

public class HighSpeedDetector implements AggregateFunction<KafkaRecord, ArrayList<KafkaRecord>, KafkaRecord> {

    private static final long serialVersionUID = 1L;

    private static final double HIGH_SPEED = 100.00;

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

        String key = accumulator.get(0).key;
        String alertMessage = "";
        Double mphTotal = 0.0;
        int numRecords = 0;

        // Look through all records
        for (KafkaRecord record : accumulator) {
            mphTotal += (double) record.data.get("mph");
            numRecords++;
        }

        if ((mphTotal / numRecords) > HIGH_SPEED) {
            alertMessage = "Speed too high!";
        }

        return new KafkaRecord(key, alertMessage);
    }

    @Override
    public ArrayList<KafkaRecord> merge(ArrayList<KafkaRecord> oldRecords, ArrayList<KafkaRecord> newRecords) {
        oldRecords.addAll(newRecords);
        return oldRecords;
    }

}
