package data;

import java.io.Serializable;

public class Stats implements Serializable {

    private static int DAYS = 7;

    private Integer min_confirmed_cases;
    private Integer max_confirmed_cases;
    private Integer min_cured;
    private Integer max_cured;
    private Integer min_swabds;
    private Integer max_swabds;


    public Integer getMin_confirmed_cases() {
        return min_confirmed_cases;
    }

    public void setMin_confirmed_cases(Integer min_confirmed_cases) {
        this.min_confirmed_cases = min_confirmed_cases;
    }

    public Integer getMax_confirmed_cases() {
        return max_confirmed_cases;
    }

    public void setMax_confirmed_cases(Integer max_confirmed_cases) {
        this.max_confirmed_cases = max_confirmed_cases;
    }

    public Integer getMin_cured() {
        return min_cured;
    }

    public void setMin_cured(Integer min_cured) {
        this.min_cured = min_cured;
    }

    public Integer getMax_cured() {
        return max_cured;
    }

    public void setMax_cured(Integer max_cured) {
        this.max_cured = max_cured;
    }

    public Integer getMin_swabds() {
        return min_swabds;
    }

    public void setMin_swabds(Integer min_swabds) {
        this.min_swabds = min_swabds;
    }

    public Integer getMax_swabds() {
        return max_swabds;
    }

    public void setMax_swabds(Integer max_swabds) {
        this.max_swabds = max_swabds;
    }


    public Double getAvg_swabds() {
        return (double) (max_swabds - min_swabds) / DAYS;
    }


    public Double getAvg_cured() {
        return (double) (max_cured - min_cured) / DAYS;
    }

    public Double getAvg_confirmed_cases() {
        return (double) (max_confirmed_cases - min_confirmed_cases) / DAYS;
    }


}
