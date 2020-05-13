package helpers;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class Common {

    public static final Calendar calendar = Calendar.getInstance();

    public static Integer getWeekFrom(String date) {

        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");

        try {
            Date d = df.parse(date);
            calendar.setTime(d);
            return calendar.get(Calendar.WEEK_OF_YEAR);

        } catch (ParseException e) {
            e.printStackTrace();
        }

        return -1;
    }


}
