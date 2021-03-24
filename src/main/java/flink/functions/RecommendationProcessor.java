package flink.functions;

import com.google.gson.JsonObject;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import flink.kafka_utility.KafkaRecord;

public class RecommendationProcessor extends ProcessFunction<KafkaRecord, KafkaRecord> {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    @Override
    public void processElement(KafkaRecord record, ProcessFunction<KafkaRecord, KafkaRecord>.Context context,
            Collector<KafkaRecord> out) throws Exception {
        KafkaRecord outRecord = new KafkaRecord();
        JsonObject key = new JsonObject();
        JsonObject data = new JsonObject();

        key.addProperty("id", record.data.get("id").getAsString());

        Boolean engineWarning = record.data.get("engineWarning").getAsBoolean();
        Boolean breaksWarning = record.data.get("breaksWarning").getAsBoolean();
        Boolean lightingSystemFailure = record.data.get("lightingSystemFailure").getAsBoolean();
        Float fuelLevel = record.data.get("fuelLevel").getAsFloat();
        Float oilLevel = record.data.get("oilLevel").getAsFloat();
        Float estimatedRange = record.data.get("estimatedRange").getAsFloat();
        Float temperatureInside = record.data.get("temperatureInside").getAsFloat();

        if(engineWarning || breaksWarning || lightingSystemFailure){ 
            data.addProperty("partsWarning", "Please look for a workshop to check your car."); //TODO Liste von Werkstätten, Liste von Slots 
        }
        if(estimatedRange < 100 || fuelLevel < 20){
            data.addProperty("estimatedRange", "Your estimated range is low. Here is a list of gas stations with cheap prices for your fuel type"); //TODO Fuel type, position, liste holen
        }
        if(oilLevel < 20){
            data.addProperty("oilLevel", "Seems like your oil level is low. Please refill as soon as possible.");
        }
        if(temperatureInside > 25 || temperatureInside < 15){
            data.addProperty("oilLevel", "Seems your temperature inside is odd.");
        }

    }

}
