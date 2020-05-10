package utils;

import java.io.Serializable;

public class Covid1Data implements Serializable {

    private String data;
    private String stato;
    private int ricoverati_con_sintomi;
    private int terapia_intensiva;
    private int totale_ospedalizzati;
    private int isolamento_domiciliare;
    private int totale_positivi;
    private int variazione_totale_positivi;
    private int nuovi_positivi;
    private int dimessi_guariti;
    private int deceduti;
    private int totale_casi;
    private int tamponi;
    private int casi_testati;


    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public String getStato() {
        return stato;
    }

    public void setStato(String stato) {
        this.stato = stato;
    }

    public int getRicoverati_con_sintomi() {
        return ricoverati_con_sintomi;
    }

    public void setRicoverati_con_sintomi(int ricoverati_con_sintomi) {
        this.ricoverati_con_sintomi = ricoverati_con_sintomi;
    }

    public int getTerapia_intensiva() {
        return terapia_intensiva;
    }

    public void setTerapia_intensiva(int terapia_intensiva) {
        this.terapia_intensiva = terapia_intensiva;
    }

    public int getTotale_ospedalizzati() {
        return totale_ospedalizzati;
    }

    public void setTotale_ospedalizzati(int totale_ospedalizzati) {
        this.totale_ospedalizzati = totale_ospedalizzati;
    }

    public int getIsolamento_domiciliare() {
        return isolamento_domiciliare;
    }

    public void setIsolamento_domiciliare(int isolamento_domiciliare) {
        this.isolamento_domiciliare = isolamento_domiciliare;
    }

    public int getTotale_positivi() {
        return totale_positivi;
    }

    public void setTotale_positivi(int totale_positivi) {
        this.totale_positivi = totale_positivi;
    }

    public int getVariazione_totale_positivi() {
        return variazione_totale_positivi;
    }

    public void setVariazione_totale_positivi(int variazione_totale_positivi) {
        this.variazione_totale_positivi = variazione_totale_positivi;
    }

    public int getNuovi_positivi() {
        return nuovi_positivi;
    }

    public void setNuovi_positivi(int nuovi_positivi) {
        this.nuovi_positivi = nuovi_positivi;
    }

    public int getDimessi_guariti() {
        return dimessi_guariti;
    }

    public void setDimessi_guariti(int dimessi_guariti) {
        this.dimessi_guariti = dimessi_guariti;
    }

    public int getDeceduti() {
        return deceduti;
    }

    public void setDeceduti(int deceduti) {
        this.deceduti = deceduti;
    }

    public int getTotale_casi() {
        return totale_casi;
    }

    public void setTotale_casi(int totale_casi) {
        this.totale_casi = totale_casi;
    }

    public int getTamponi() {
        return tamponi;
    }

    public void setTamponi(int tamponi) {
        this.tamponi = tamponi;
    }

    public int getCasi_testati() {
        return casi_testati;
    }

    public void setCasi_testati(int casi_testati) {
        this.casi_testati = casi_testati;
    }


    @Override
    public String toString() {
        return "Covid1Data{" +
                "data='" + data + '\'' +
                ", dimessi_guariti=" + dimessi_guariti +
                ", tamponi=" + tamponi +
                '}';
    }
}
