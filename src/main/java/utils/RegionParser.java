package utils;

import model.Region;

public class RegionParser {

    public static Region parseCSVRegion(String line) {

        String[] data = line.split(",");
        return new Region(data[0], data[1]);

    }

}