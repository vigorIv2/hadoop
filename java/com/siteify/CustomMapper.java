package com.siteify;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class CustomMapper extends Mapper<CustomKey, CustomValue, Text, Text> {
        
          protected void map(CustomKey key, CustomValue value, Context context)
              throws java.io.IOException, InterruptedException {
        	  
            String sensor = key.getSensorType().toString();
            
            if(sensor.toLowerCase().equals("a")){
            	context.write(value.getValue1(),value.getValue2());
            }
            		
          }  
}