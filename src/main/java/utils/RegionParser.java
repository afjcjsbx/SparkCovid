package utils;

import data.Region;

public class RegionParser {

    public static Region parseCSVRegion(String line) {

        String[] data = line.split(",");
        return new Region(data[0], data[1]);

    }

}