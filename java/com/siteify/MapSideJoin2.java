package com.siteify;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapSideJoin2 {

	public static class PatentClassMap extends Mapper<LongWritable, Text, Text, Text> {

		private Map<String, String> abMap = new HashMap<String, String>();
		private Text outputKey = new Text();
		private Text outputValue = new Text();

		protected void setup(Context context) throws java.io.IOException, InterruptedException {

			Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());

			for (Path p : files) {
				if (p.getName().equals("list_of_classes.txt")) {
					BufferedReader reader = new BufferedReader(new FileReader(p.toString()));
					String line = reader.readLine();
					while (line != null) {
						String[] tokens = line.split("\t");
						String classId = Integer.parseInt(tokens[0]) + "";
						String className = tokens[1];
						abMap.put(classId, className);
						line = reader.readLine();
					}
				}
			}
			if (abMap.isEmpty()) {
				throw new IOException("Unable to load Abbrevation data.");
			}

		}

		  protected void map(LongWritable key, Text value, Context context)
		            throws java.io.IOException, InterruptedException {
			String line = value.toString();
			String[] tokens = line.split(",");
			String patentStr = tokens[0];
			String patentClassId = tokens[9];

			outputKey.set(patentStr);
			outputValue.set(abMap.get(patentClassId));
			context.write(outputKey, outputValue);

		}
	}

	 public static void main(String[] args) 
             throws IOException, ClassNotFoundException, InterruptedException {

		 Job job = new Job();
		job.setJobName("MapSideJoin2");
		job.setJarByClass(MapSideJoin2.class);
		job.setNumReduceTasks(0);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(0);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		job.setMapperClass(PatentClassMap.class);

	    try{
	    DistributedCache.addCacheFile(new URI(args[1]), job.getConfiguration());
	    }catch(Exception e){
	    	System.out.println(e);
	    }
		job.waitForCompletion(true);
	}
}