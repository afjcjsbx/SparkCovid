package data;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Covid2DataInner implements Serializable {

    private String day;
    private Integer confirmed_cases;

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }

    public Integer getConfirmed_cases() {
        return confirmed_cases;
    }

    public void setConfirmed_cases(Integer confirmed_cases) {
        this.confirmed_cases = confirmed_cases;
    }


    @Override
    public String toString() {
        return "{" +
                "day=" + day +
                ", =" + confirmed_cases +
                '}';
    }

}
