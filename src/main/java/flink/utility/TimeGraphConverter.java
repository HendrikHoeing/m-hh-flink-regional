package flink.utility;

import java.text.SimpleDateFormat;
import java.util.Date;

public class TimeGraphConverter {
    
    public static String convertMillisToGraphFormat(long millis){
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SS").format(new Date(millis));
    }
}