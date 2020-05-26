package com.afjcjsbx.sabdcovid.utils;

import com.afjcjsbx.sabdcovid.model.Region;

import java.io.Serializable;

public class RegionParser implements Serializable {

    public static Region parseCSVRegion(String line) {

        String[] data = line.split(",");
        return new Region(data[0], data[1]);

    }

}