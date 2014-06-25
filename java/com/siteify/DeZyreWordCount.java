package com.siteify;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

	
public class DeZyreWordCount {
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private static final char[] punctuation = 
				new char[]{'‘','’','-','—','–','_','=','\'','`','+',',', ';', ':', '\'','"','!','?','@','&','(',')','[',']','{','}','\\','/','.'};	
		
	    public void map(LongWritable key, Text value, Context context) 
	    	throws IOException,InterruptedException {
	        String line = value.toString();
	        
	        for( char c : punctuation ) // clean up punctuation  
	        	line = line.replace(c,' ');
	        
	        StringTokenizer tokenizer = new StringTokenizer(line);
	        while (tokenizer.hasMoreTokens()) {
	        	word.set(tokenizer.nextToken().toLowerCase().trim());
	        	context.write(word, one);
	        }
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
		
		job.setJarByClass(DeZyreWordCount.class);
		
		
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