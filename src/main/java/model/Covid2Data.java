package model;

import java.io.Serializable;
import java.util.ArrayList;

public class Covid2Data implements Serializable {

    private String state;
    private double lat;
    private double lng;

    private ArrayList<Integer> cases;

    public Covid2Data() {
        this.cases = new ArrayList<>();;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
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

    public ArrayList<Integer> getCases() {
        return cases;
    }

    public void setCases(ArrayList<Integer> cases) {
        this.cases = cases;
    }

    @Override
    public String toString() {
        return "Covid2Data{" +
                "state='" + state + '\'' +
                ", lat=" + lat +
                ", lng=" + lng +
                ", days=" + cases +
                '}';
    }
}
