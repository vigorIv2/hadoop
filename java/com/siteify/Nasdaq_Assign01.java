package com.siteify;

import java.io.IOException;
//import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Nasdaq_Assign01 {
		
	public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
//exchange,stock_symbol,date,stock_price_open,stock_price_high,stock_price_low,stock_price_close,stock_volume,stock_price_adj_close
//NASDAQ,QGEN,2010-02-08,20.83,20.90,20.64,20.66,1006900,20.66
		
//		private final static IntWritable one = new IntWritable(1);
//		private Text word = new Text();
		int stock_id=1;
		int volume_id=7;
		
	    public void map(LongWritable key, Text value, Context context) 
	    	throws IOException,InterruptedException {
	        String line = value.toString();
	        String[] cols = line.split(","); // split to columns
	        if (cols[volume_id].equals("stock_volume"))
	        	return; // ignore header row
	        Text stock=new Text(cols[stock_id]);
	        
	        IntWritable volume=new IntWritable(Integer.valueOf(cols[volume_id]).intValue());		
	        context.write(stock, volume);
	    }
	}
	
	public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException {
	        int sum = 0;
	        for (IntWritable val : values) {
	        	sum += val.get();
	        }
	        context.write(key, new IntWritable(sum));
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = new Job(conf, "wordcount");
		
		job.setJarByClass(Nasdaq_Assign01.class);
		
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
			
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
		job.waitForCompletion(true);
	}
}

