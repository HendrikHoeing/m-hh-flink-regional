package flink;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class AnomalyDetectorWindow implements WindowFunction<KafkaRecord, KafkaRecord, String, TimeWindow> {

    private static final long serialVersionUID = 1L;

    @Override
    public void apply(String key, TimeWindow window, Iterable<KafkaRecord> record, Collector<KafkaRecord> out)
            throws Exception {
        // TODO Auto-generated method stub
        System.out.println(key);
        System.out.println(window);
        System.out.println(record);

        // TODO

    }

}