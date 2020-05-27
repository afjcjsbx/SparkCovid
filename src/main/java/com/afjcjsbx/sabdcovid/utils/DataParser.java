package com.afjcjsbx.sabdcovid.utils;

import com.afjcjsbx.sabdcovid.model.Covid1Data;
import com.afjcjsbx.sabdcovid.model.Covid2Data;
import com.afjcjsbx.sabdcovid.enums.Continent;

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

        // Tolgo la virgola in alcuni stati che la contengono come nome composto delimitato dale virgolette
        int i = csvLine.indexOf("\"");
        if(i != -1){
            int f = csvLine.substring(i +1).indexOf("\"");
            String state = csvLine.substring(i + 1, f + i + 1);
            state = state.replace(",", "");

            csvLine = csvLine.substring(0, i) + state + csvLine.substring(f+i+2);
        }

        String[] data = csvLine.split(",");

        Covid2Data covid2Data = new Covid2Data();
        //State not set use country
        covid2Data.setState(data[0].isEmpty() ? data[1] : data[0]);

        ArrayList<Integer> cases = new ArrayList<>();
        int n1, n2, res;

        for(i = 4; i < data.length; i++) {
            if (i == 4) {
                cases.add(Integer.parseInt(data[i]));
            } else {
                n2 = Integer.parseInt(data[i]);
                n1 = Integer.parseInt(data[(i - 1)]);
                if (n2 < n1)
                    res = 0;
                else
                    res = n2 - n1;
                cases.add(res);
            }
        }

        covid2Data.setCases(cases);


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