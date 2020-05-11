package utils;

import data.Covid2Data;
import data.Covid2DataInner;

import java.util.ArrayList;

public class DataParser {

    public static Covid1Data parseCSV(String csvLine) {

        String[] csvValues = csvLine.split(",", -1);

        String[] timestamp = csvValues[0].split("T", -1);

        Covid1Data cdata = new Covid1Data();
        cdata.setData(timestamp[0]);
        cdata.setDimessi_guariti(Integer.parseInt(csvValues[8]));
        cdata.setTamponi(Integer.parseInt(csvValues[12]));

            return cdata;
    }

    public static Covid2Data parseCSVcovid2data(String csvLine, Integer days, String[] header){
        String[] csvValues = csvLine.split(",", -1);
        ArrayList<Covid2DataInner> data = new ArrayList<>();

        Covid2Data cdata = new Covid2Data();
        cdata.setStato(csvValues[0]);
        cdata.setRegione(csvValues[1]);
        cdata.setLatitudine(Integer.parseInt(csvValues[2]));
        cdata.setLongitudine(Integer.parseInt(csvValues[3]));
        for (int i = 4; i < days; i++){
            Covid2DataInner futher_info = new Covid2DataInner();
            futher_info.setDay(header[i]);
            futher_info.setConfirmed_cases(Integer.parseInt(csvValues[i]));
            data.add(futher_info);
        }
        cdata.setData(data);
        return cdata;
    }

}