package utils;

import java.util.Calendar;

public class ConvertData {
    public static Calendar convert(String data){
        String[] dataSplitted = data.split("/", -1);
        int day = Integer.parseInt(dataSplitted[1]);
        int month = Integer.parseInt(dataSplitted[0]);
        int year = Integer.parseInt(dataSplitted[2]);

        year += 2000;

        Calendar cal = Calendar.getInstance();
        cal.set(year, month, day);
        return cal;
    }

    public static Calendar addMonth(Calendar date) {

        date.add(Calendar.MONTH, 1);
        return date;
    }

    public static Calendar goInitNextMonth(Calendar cal) {

        cal.set(Calendar.DAY_OF_MONTH, 1);
        cal.add(Calendar.MONTH, 1);
        return cal;
    }

    public static Calendar nextMonth(Calendar cal) {

        cal.add(Calendar.MONTH, 1);
        return cal;
    }

    public static Calendar goToEndOfTheMonth(Calendar cal){
        cal.set(Calendar.DAY_OF_MONTH, 31);
        return cal;
    }

}
