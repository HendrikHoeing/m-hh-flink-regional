package flink.functions;

import com.google.gson.JsonObject;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import flink.kafka_utility.KafkaRecord;

public class HealthProcessor extends ProcessFunction<KafkaRecord, KafkaRecord> {

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

        Boolean drugged = record.data.get("drugged").getAsBoolean();
        Boolean drivingOnMarkers = record.data.get("drivingOnMarkers").getAsBoolean();
        Boolean rapidSteeringWheelMovement = record.data.get("rapidSteeringWheelMovement").getAsBoolean();
        Boolean forwardCollisionWarning = record.data.get("forwardCollisionWarning").getAsBoolean();
        Float infotainmentVolume = record.data.get("infotainmentVolume").getAsFloat();
        Float oxygenLevel = record.data.get("oxygenLevel").getAsFloat();
        Float temperatureInside = record.data.get("temperatureInside").getAsFloat();
        Float travelTimeTotal = record.data.get("travelTimeTotal").getAsFloat();

        if (drugged) {
            data.addProperty("druggedWarning", "Seems like you had one too much. Please pull over and stop driving!");
        }
        if (drivingOnMarkers && rapidSteeringWheelMovement && (travelTimeTotal > 200)) {
            data.addProperty("sleepyWarning", "Seems like you are tired. Take a break or stop driving!");
        }
        if (forwardCollisionWarning && (infotainmentVolume > 80)) {
            data.addProperty("aggressiveWarning",
                    "Seems like you are in a hurry or bad mood. Please drive consciously to avoid harm to yourself and others");
        }
        if ((temperatureInside > 25) || (oxygenLevel < 20)) {
            data.addProperty("airWarning",
                    "Seems like you have low quality air. Please consider airing your car or turn down the heat.");
        }

        outRecord.key = key;
        outRecord.data = data;
        
        out.collect(outRecord);
    }

}
