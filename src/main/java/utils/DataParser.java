package utils;

import scala.Tuple3;

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


}