package utils;

import java.util.ArrayList;

public class DataParser {

    public static ArrayList<Covid1Data> parseCSV(String csvLine) {

        ArrayList<Covid1Data> covid1Data = new ArrayList<>();
        String[] csvValues = csvLine.split(",", -1);

        String[] timestamp = csvValues[0].split("T", -1);

        Covid1Data cdata = new Covid1Data();
        cdata.setData(timestamp[0]);
        cdata.setDimessi_guariti(Integer.parseInt(csvValues[8]));
        cdata.setTamponi(Integer.parseInt(csvValues[12]));

        if (!cdata.getData().equals(""))
            covid1Data.add(cdata);


        return covid1Data;
    }
}