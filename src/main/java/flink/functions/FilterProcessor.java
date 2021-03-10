package flink.functions;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import com.google.gson.JsonObject;
import flink.kafka_utility.KafkaRecord;

public class FilterProcessor extends KeyedProcessFunction<String, KafkaRecord, KafkaRecord> {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    private String region;

    public FilterProcessor(String region) {
        this.region = region;
    }

    /**
     * Filters each kafka records and outputs record with selected values for global analysis
     */
    @Override
    public void processElement(KafkaRecord record,
            KeyedProcessFunction<String, KafkaRecord, KafkaRecord>.Context context, Collector<KafkaRecord> out)
            throws Exception {
        try {
            JsonObject filteredData = new JsonObject();

            if (region.equals("eu")) {
                filteredData.addProperty("consumptionKm", record.data.get("consumptionKm").getAsFloat());
                filteredData.addProperty("co2Km", record.data.get("co2Km").getAsFloat());
            } else {
                filteredData.addProperty("consumptionMile", record.data.get("consumptionMile").getAsFloat());
                filteredData.addProperty("co2Mile", record.data.get("co2Mile").getAsFloat());
            }

            filteredData.addProperty("geoChip", record.data.get("geoChip").getAsBoolean());
            filteredData.addProperty("breaksHealth", record.data.get("breaksHealth").getAsFloat());
            filteredData.addProperty("engineHealth", record.data.get("engineHealth").getAsFloat());
            filteredData.addProperty("tireHealth", record.data.get("tireHealth").getAsFloat());
            filteredData.addProperty("mufflerHealth", record.data.get("mufflerHealth").getAsFloat());
            filteredData.addProperty("gearsHealth", record.data.get("gearsHealth").getAsFloat());
            filteredData.addProperty("batteryHealth", record.data.get("batteryHealth").getAsFloat());

            record.data = filteredData;
            out.collect(record);

        } catch (Exception e) {
            return;
        }

    }

}
