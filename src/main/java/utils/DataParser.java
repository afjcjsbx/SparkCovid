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

    public static Covid2Data parseCSVcovid2data(String csvLine, String header){

        //pARSER
        String[] data = csvLine.split(",");

        Covid2Data covid2Data = new Covid2Data();
        covid2Data.setState(data[0]);
        covid2Data.setCountry(data[1]);
        covid2Data.setLat(Double.parseDouble(data[2]));
        covid2Data.setLng(Double.parseDouble(data[3]));

        String[] head = header.split(",");


        for(int i = 4; i < data.length; i++){
            Covid2DataInner covid2DataInner = new Covid2DataInner();
            covid2DataInner.setDay(head[i]);
            covid2DataInner.setConfirmed_cases(Integer.parseInt(data[i]));

            covid2Data.getDays().add(covid2DataInner);
        }

        return covid2Data;
    }
}