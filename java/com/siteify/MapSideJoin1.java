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

public class MapSideJoin1 {
	
	
	public static class MyMapper extends Mapper<LongWritable,Text, Text, Text> {
        
		
		private Map<String, String> deptMap = new HashMap<String, String>();
				private Text outputKey = new Text();
				private Text outputValue = new Text();
		
		protected void setup(Context context) throws java.io.IOException, InterruptedException{
			Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			
			
			for (Path p : files) {
				if (p.getName().equals("dept.txt")) {
					BufferedReader reader = new BufferedReader(new FileReader(p.toString()));
					String line = reader.readLine();
					while(line != null) {
						String[] tokens = line.split(",");
						String deptId = tokens[0];
						String deptName = tokens[1];
						deptMap.put(deptId, deptName);
						line = reader.readLine();
					}
				}
			}
			if (deptMap.isEmpty()) {
				throw new IOException("Unable to load Abbrevation data.");
			}
		}

		
        protected void map(LongWritable key, Text value, Context context)
            throws java.io.IOException, InterruptedException {
        	
        	
        	String row = value.toString();
        	String[] tokens = row.split(",");
        	String deptId = tokens[2];
        	String deptName = deptMap.get(deptId);
        	outputKey.set(deptName);
        	outputValue.set(row);
      	  	context.write(outputKey,outputValue);
        }  
}
	
	
  public static void main(String[] args) 
                  throws IOException, ClassNotFoundException, InterruptedException {
    
    Job job = new Job();
    job.setJarByClass(MapSideJoin1.class);
    job.setJobName("MapSideJoin1");
    job.setNumReduceTasks(0);
    
    try{
    DistributedCache.addCacheFile(new URI("/dept.txt"), job.getConfiguration());
    }catch(Exception e){
    	System.out.println(e);
    }
    
    job.setMapperClass(MyMapper.class);
    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.waitForCompletion(true);
    
    
  }
}