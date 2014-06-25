package com.siteify;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;


public class CustomMain {

  public static void main(String[] args) 
                  throws IOException, ClassNotFoundException, InterruptedException {
    if (args.length != 2) {
      System.err.println("Usage: <input path> <output path>");
      System.exit(-1);
    }
    
    Job job = new Job();
    job.setJarByClass(CustomMain.class);
    job.setJobName("CustomTest");
    job.setNumReduceTasks(0);
    job.setMapperClass(CustomMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setInputFormatClass(CustomInputFormat.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.waitForCompletion(true);
  }
}