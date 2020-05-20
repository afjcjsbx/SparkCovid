package spark.helpers;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
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

    public static Integer getWeekFrom2(String date) {

        SimpleDateFormat df = new SimpleDateFormat("MM/dd/yy");

        try {
            Date d = df.parse(date);
            calendar.setTime(d);
            return calendar.get(Calendar.WEEK_OF_YEAR);

        } catch (ParseException e) {
            e.printStackTrace();
        }

        return -1;
    }


    public static Integer getMonthFromDate(String date) {

        SimpleDateFormat df = new SimpleDateFormat("MM/dd/yyyy");

        // Aggiungo l'anno manualmente perché questo anno (2020) è bisestile
        // altrimenti mi conta il mese di febbraio di 28 giorni perché
        // l'anno nel dataset non è indicato
        try {
            Date d = df.parse(date + "/2020");
            calendar.setTime(d);
            return calendar.get(Calendar.MONTH);

        } catch (ParseException e) {
            e.printStackTrace();
        }

        return -1;
    }



}
