package flink;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction.Context;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction.OnTimerContext;
import org.apache.flink.util.Collector;


public class AnomalyDetector extends KeyedProcessFunction<String, KafkaRecord, KafkaRecord> {

    private static final long serialVersionUID = 1L;

    private static final double HIGH_TEMP_TIRES = 70.00;
    private static final double HIGH_TEMP_BREAKS = 60.00;
    private static final double HIGH_TEMP_ENGINE = 50.00;
    
    private static final double HIGH_SPEED = 140.00;


    private transient ValueState<Boolean> flagState;
    private transient ValueState<Long> timerState;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<>(
                "flag",
                Types.BOOLEAN);
        flagState = getRuntimeContext().getState(flagDescriptor);

        ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<>(
                "timer-state",
                Types.LONG);
        timerState = getRuntimeContext().getState(timerDescriptor);
    }

    @Override
    public void processElement(
    		KafkaRecord record,
            Context context,
            Collector<KafkaRecord> collector) throws Exception {

//        // Get the current state for the current key
        Boolean lastBreakTempHigh = flagState.value();
        
        System.out.println("Key: " + record.key + " || Value: " + record.value);

//        // Check if the flag is set
//        if (lastBreakTempHigh != null) {
//            if (record.value == HIGH_TEMP_BREAKS) {
//                //Output an alert downstream
//                Alert alert = new Alert();
//                alert.setId(record.getAccountId());
//
//                collector.collect(alert);
//            }
            // Clean up our state
//            cleanUp(context);
//        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<KafkaRecord> out) {
        // remove flag after 1 minute
        timerState.clear();
        flagState.clear();
    }

    private void cleanUp(Context ctx) throws Exception {
        // delete timer
        Long timer = timerState.value();
        ctx.timerService().deleteProcessingTimeTimer(timer);

        // clean up all state
        timerState.clear();
        flagState.clear();
    }

}

