package com.siteify;

import java.io.IOException;
//import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Nasdaq_Assign3{
		
	public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
//exchange,stock_symbol,date,stock_price_open,stock_price_high,stock_price_low,stock_price_close,stock_volume,stock_price_adj_close
//NASDAQ,QGEN,2010-02-08,20.83,20.90,20.64,20.66,1006900,20.66
		
//		private final static IntWritable one = new IntWritable(1);
//		private Text word = new Text();
		int stock_id=1;
		int volume_id=7;
		int date_id=2;
		int price_id=3;
		
	    public void map(LongWritable key, Text value, Context context) 
	    	throws IOException,InterruptedException {
	        String line = value.toString();
	        String[] cols = line.split(","); // split to columns
	        if (cols[volume_id].equals("stock_volume"))
	        	return; // ignore header row
	        
	        IntWritable volume=new IntWritable(Integer.valueOf(cols[volume_id]).intValue());	
//	        IntWritable price=new IntWritable((int)(Float.valueOf(cols[price_id]).floatValue()*100));	
	        String stock=cols[stock_id];
	        String month=cols[date_id].substring(0,7);
	        Text stock_month=new Text(stock+'.'+month); 
        	context.write(stock_month, volume);
	    }
	}
	
	public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private String current_stock = null;
		private int current_stock_volume = 0;
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException {
	        int sum_month = 0;
	        int dot_pos=key.toString().indexOf(".");
	        if ( current_stock != null )
	        	if ( !key.toString().substring(0, dot_pos).equals(current_stock) ) { // if stock changes - spit out previous sum
	    	        context.write(new Text(current_stock), new IntWritable(current_stock_volume));
	        		current_stock_volume = 0; // reset it for new stock
	        	}
	        for (IntWritable val : values) {
	        	sum_month += val.get();
	        	current_stock_volume += val.get();
	        }
	        current_stock = key.toString().substring(0, dot_pos);
	        context.write(key, new IntWritable(sum_month));
		}
	
		// write info for last stock into context
		protected void cleanup(Context context) throws IOException, InterruptedException {
	        context.write(new Text(current_stock), new IntWritable(current_stock_volume));
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = new Job(conf, "wordcount");
		
		job.setJarByClass(Nasdaq_Assign3.class);
		
		
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

