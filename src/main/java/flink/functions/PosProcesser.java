package flink.functions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.curator4.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import flink.kafka_utility.KafkaRecord;

public class PosProcesser extends ProcessWindowFunction<KafkaRecord, Tuple2<Float, Float>, String, TimeWindow> {
    private static final long serialVersionUID = 1L;

    @Override
    public void process(String key,
            ProcessWindowFunction<KafkaRecord, Tuple2<Float, Float>, String, TimeWindow>.Context context,
            Iterable<KafkaRecord> elements, Collector<Tuple2<Float, Float>> out) throws Exception {

        KafkaRecord latestRecord = Iterables.getLast(elements);

        Float lat = latestRecord.data.get("pos").getAsJsonObject().get("lat").getAsFloat();
        Float lon = latestRecord.data.get("pos").getAsJsonObject().get("lon").getAsFloat();
        out.collect(new Tuple2<>(lat, lon));

    }
}
