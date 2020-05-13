package utils;

import data.Covid2Data;
import enums.Continent;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class DataParser {

    public static Covid1Data parseCSV(String csvLine) {

        String[] csvValues = csvLine.split(",", -1);

        String[] timestamp = csvValues[0].split("T", -1);

        Covid1Data cdata = new Covid1Data();
        cdata.setData(timestamp[0]);
        cdata.setDimessi_guariti(Integer.parseInt(csvValues[9]));
        cdata.setTamponi(Integer.parseInt(csvValues[12]));

        return cdata;
    }

    public static Covid2Data parseCSVcovid2data(String csvLine, String header){

        //PARSER
        String[] data = csvLine.split(",");

        Covid2Data covid2Data = new Covid2Data();
        covid2Data.setState(data[0]);
        covid2Data.setCountry(data[1]);
        covid2Data.setLat(Double.parseDouble(data[2]));
        covid2Data.setLng(Double.parseDouble(data[3]));

        ArrayList<Integer> cases = new ArrayList<>();
        int n1, n2, res;

        for(int i = 4; i < data.length; i++) {
            if (i == 4) {
                cases.add(Integer.parseInt(data[i]));
            } else {
                n2 = Integer.parseInt(data[i]);
                n1 = Integer.parseInt(data[(i - 1)]);
                if (n2 < n1)
                    res = 0;
                else
                    res = n2 - n1;
                covid2Data.getCases().add(res);
            }
        }


        return covid2Data;
    }

    public static HashMap<String, String> countryParser(String csvFile) throws IOException, FileNotFoundException {

        String line =  null;
        HashMap<String, String> countries = new HashMap<String, String>();

        BufferedReader br = new BufferedReader(new FileReader(csvFile));

        while((line=br.readLine())!=null){
            String str[] = line.split(",");
            if (str[0].equals("name")) {
            } else {
                countries.put(str[0], str[5]);
            }
        }
        return countries;
    }

    public static Continent mappingContinent(String continent){
        if (continent.equals("Europe")){
            return Continent.EUROPE;
        } else if (continent.equals("Americas")){
            return Continent.AMERICAS;
        } else if (continent.equals("Africa")){
            return Continent.AFRICA;
        } else if (continent.equals("Asia")){
            return Continent.ASIA;
        } else if (continent.equals("Oceania")){
            return Continent.OCEANIA;
        } else {
            return Continent.ANTARCTICA;
        }
    }
}