package com.siteify;

import java.io.IOException;
//import java.util.*;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Patent_Assign5a{
		
	public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
//		"PATENT","GYEAR","GDATE","APPYEAR","COUNTRY","POSTATE","ASSIGNEE","ASSCODE","CLAIMS","NCLASS","CAT","SUBCAT","CMADE","CRECEIVE","RATIOCIT","GENERAL","ORIGINAL","FWDAPLAG","BCKGTLAG","SELFCTUB","SELFCTLB","SECDUPBD","SECDLWBD"
//		3070801,1963,1096,,"BE","",,1,,269,6,69,,1,,0,,,,,,,
//		3070802,1963,1096,,"US","TX",,1,,2,6,63,,0,,,,,,,,,
//		3070803,1963,1096,,"US","IL",,1,,2,6,63,,9,,0.3704,,,,,,,
//		3070804,1963,1096,,"US","OH",,1,,2,6,63,,3,,0.6667,,,,,,,
		
		int gyear_id=1;
		IntWritable one = new IntWritable(1);  
		int line_num = 0;
		
	    public void map(LongWritable key, Text value, Context context) 
	    	throws IOException,InterruptedException {
	    	if (line_num > 0) { // skip header
		        String line = value.toString();
		        String[] cols = line.split(","); // split to columns
		        if  (cols.length > 2) {
		        	Text gyear = new Text(cols[gyear_id]);
		        	context.write(gyear, one);
		        }
	    	}
	    	line_num++;
	    }
	}
	
	public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException {
	        int sum_year = 0;
	        for (IntWritable val : values) 
	        	sum_year += val.get();
	        context.write(key, new IntWritable(sum_year));
		}
	
		// write info for last stock into context
		protected void cleanup(Context context) throws IOException, InterruptedException {
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = new Job(conf, "wordcount");
		
		job.setJarByClass(Patent_Assign5a.class);
		
		
		job.setMapperClass(MyMapper.class);
		job.setCombinerClass(MyReducer.class);
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

