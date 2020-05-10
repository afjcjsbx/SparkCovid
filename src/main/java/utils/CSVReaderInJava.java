package utils;

import java.awt.print.Book;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class CSVReaderInJava {

    public List<Covid1Data> readDataFromCSV(String fileName) {
        List<Covid1Data> data = new ArrayList<Covid1Data>();
        Path pathToFile = Paths.get(fileName);

        // create an instance of BufferedReader
        // using try with resource, Java 7 feature to close resources
        try (BufferedReader br = Files.newBufferedReader(pathToFile,
                StandardCharsets.US_ASCII)) {

            // read the first line from the text file
            String line = br.readLine();

            // loop until all lines are read
            while (line != null) {

                // use string.split to load a string array with the values from
                // each line of
                // the file, using a comma as the delimiter
                String[] attributes = line.split(",");

                Covid1Data book = null;

                // adding book into ArrayList
                data.add(book);

                // read next line before looping
                // if end of file reached, line would be null
                line = br.readLine();
            }

        } catch (IOException ioe) {
            ioe.printStackTrace();
        }

        return data;
    }

    public static Covid1Data createData(String line) {

        String[] metadata = line.split(",", -1);

        String d = metadata[0];
        String stato = metadata[1];;
        int ricoverati_con_sintomi = Integer.parseInt(metadata[2]);
        int terapia_intensiva = Integer.parseInt(metadata[3]);
        int totale_ospedalizzati = Integer.parseInt(metadata[4]);
        int isolamento_domiciliare = Integer.parseInt(metadata[5]);
        int totale_positivi = Integer.parseInt(metadata[6]);
        int variazione_totale_positivi = Integer.parseInt(metadata[17]);
        int nuovi_positivi = Integer.parseInt(metadata[8]);
        int dimessi_guariti = Integer.parseInt(metadata[9]);
        int deceduti = Integer.parseInt(metadata[10]);
        int totale_casi = Integer.parseInt(metadata[11]);
        int tamponi = Integer.parseInt(metadata[12]);
        int casi_testati = Integer.parseInt(metadata[13]);

        // create and return book of this metadata
        return null;
    }

}