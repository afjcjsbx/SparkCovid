package data;

import java.io.Serializable;
import java.util.ArrayList;

public class Covid2Data implements Serializable {
    private String stato;
    private String regione;
    private Integer latitudine;
    private Integer longitudine;
    private ArrayList<Covid2DataInner> data;

    public ArrayList<Covid2DataInner> getData() {
        return data;
    }

    public void setData(ArrayList<Covid2DataInner> data) {
        this.data = data;
    }

    public String getStato() {
        return stato;
    }

    public void setStato(String stato) {
        this.stato = stato;
    }

    public String getRegione() {
        return regione;
    }

    public void setRegione(String regione) {
        this.regione = regione;
    }

    public Integer getLatitudine() {
        return latitudine;
    }

    public void setLatitudine(Integer latitudine) {
        this.latitudine = latitudine;
    }

    public Integer getLongitudine() {
        return longitudine;
    }

    public void setLongitudine(Integer longitudine) {
        this.longitudine = longitudine;
    }
}
