package utils;

import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.text.SimpleDateFormat;

public class ConvertData {
    public static Date convert(String data){
        Date date = null;
        String[] dataSplitted = data.split("/", -1);
        int day = Integer.parseInt(dataSplitted[1]);
        int month = Integer.parseInt(dataSplitted[0]);
        int year = Integer.parseInt(dataSplitted[2]);

        year += 2000;

        SimpleDateFormat formatter = new SimpleDateFormat("dd/MMM/yyyy");
        String dateInString = day + "/" + month + "/" + year;

        try {

            date = formatter.parse(dateInString);
            System.out.println(date);
            System.out.println(formatter.format(date));

        } catch (ParseException e) {
            e.printStackTrace();
        }

        return date;
    }

    public static Calendar addMonth(Date date) throws ParseException {

        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
        Date new_date = sdf.parse(String.valueOf(date));
        Calendar cal = Calendar.getInstance();
        cal.setTime(new_date);

        return cal;
    }
}
