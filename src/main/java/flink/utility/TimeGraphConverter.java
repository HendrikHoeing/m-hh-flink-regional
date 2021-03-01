package flink.utility;

import java.text.SimpleDateFormat;
import java.util.Date;

public class TimeGraphConverter {

    /**
     * Creates data format compatible for json graph format
     * https://plotly.com/chart-studio-help/json-chart-schema/
     */
    public static String convertMillisToGraphFormat(long millis) {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SS").format(new Date(millis));
    }
}