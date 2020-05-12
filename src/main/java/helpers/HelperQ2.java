package helpers;

import org.apache.spark.api.java.function.Function2;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;


public class HelperQ2 {

    public static final Integer INITIAL_WEEK = 9;

    public static Function2<Integer, Integer, Integer> reduceSumFunction = Integer::sum;

    public static Date stringToDate(String date){
        SimpleDateFormat df = new SimpleDateFormat("MM/dd/yy");
        try {
            return df.parse(date);

        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String dateToString(Date date){
        DateFormat dateFormat = new SimpleDateFormat("MM/dd/yy");
        return dateFormat.format(date);
    }

}
