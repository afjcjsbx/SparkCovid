package data;

import org.apache.spark.sql.sources.In;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Covid2Data implements Serializable {

    private String state;
    private String country;
    private double lat;
    private double lng;
    private List<Covid2DataInner> days;

    public Covid2Data() {
        this.days = new ArrayList<Covid2DataInner>() ;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public double getLat() {
        return lat;
    }

    public void setLat(double lat) {
        this.lat = lat;
    }

    public double getLng() {
        return lng;
    }

    public void setLng(double lng) {
        this.lng = lng;
    }

    public List<Covid2DataInner> getDays() {
        return days;
    }

    @Override
    public String toString() {
        return "Covid2Data{" +
                "state='" + state + '\'' +
                ", country='" + country + '\'' +
                ", lat=" + lat +
                ", lng=" + lng +
                ", days=" + days +
                '}';
    }
}
