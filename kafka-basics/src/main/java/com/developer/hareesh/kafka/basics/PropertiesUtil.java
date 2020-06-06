package com.developer.hareesh.kafka.basics;

import java.io.IOException;
import java.util.Properties;

public class PropertiesUtil {
    private static Properties properties = null;
    static void loadProperties() {
        properties = new Properties();
        try {
            properties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("details.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getProperty(String propertyName){
        if(properties == null){
            PropertiesUtil.loadProperties();
        }
        return (String) PropertiesUtil.properties.get(propertyName);
    }

}
