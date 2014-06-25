package com.siteify;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
/*
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Mapper.Context;
*/

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Module4_Assign1 {


	public static class MyMapper1 extends Mapper<LongWritable,Text, Text, Text>  {
//		private final static IntWritable one = new IntWritable(1);
		
		enum MYCOUNTER {
			RECORD_COUNT, FILE_EXISTS, LOAD_MAP_ERROR
		}

		private Map<String, String> patMap = new HashMap<String, String>();

		protected void setup(Context context) throws java.io.IOException, InterruptedException{
			Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			System.out.println("IV DEBUG 1");			
			for (Path p : files) {
				if (p.getName().equals("./list_of_classes.txt")) {
					System.out.println("IV DEBUG 2");
					BufferedReader reader = new BufferedReader(new FileReader(p.toString()));
					String line = reader.readLine();
					while(line != null) {
						System.out.println("Line="+line);
						String[] tokens = line.split("\t");
						String categoryId = tokens[0];
						try {
							int catId = Integer.parseInt(categoryId);
							String categoryName = tokens[1];
							patMap.put(categoryId, categoryName);
						} catch (NumberFormatException nfe) {} // ignore header lines
						line = reader.readLine();
					}
					reader.close();
				}
			}
			if (patMap.isEmpty()) {
				throw new IOException("Unable to load Abbreviation data. IV");
			}
		}

/*
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {

			Path[] cacheFilesLocal = DistributedCache
					.getLocalCacheArchives(context.getConfiguration());

			for (Path eachPath : cacheFilesLocal) {

				if (eachPath.getName().toString().trim()
						.equals("departments_map.tar.gz")) {
					URI uriUncompressedFile = new File(eachPath.toString()
							+ "/departments_map").toURI();
					context.getCounter(MYCOUNTER.FILE_EXISTS).increment(1);
					loadDepartmentsMap(uriUncompressedFile, context);
				}
			}
		}
*/

//		private Text word = new Text();
/*
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				output.collect(word, one);
			}
		}
*/
		
//		"PATENT","GYEAR","GDATE","APPYEAR","COUNTRY","POSTATE","ASSIGNEE","ASSCODE","CLAIMS","NCLASS","CAT","SUBCAT","CMADE","CRECEIVE","RATIOCIT","GENERAL","ORIGINAL","FWDAPLAG","BCKGTLAG","SELFCTUB","SELFCTLB","SECDUPBD","SECDLWBD"
//		3070801,1963,1096,,"BE","",,1,,269,6,69,,1,,0,,,,,,,
//		3070802,1963,1096,,"US","TX",,1,,2,6,63,,0,,,,,,,,,
//		3070803,1963,1096,,"US","IL",,1,,2,6,63,,9,,0.3704,,,,,,,
//		3070804,1963,1096,,"US","OH",,1,,2,6,63,,3,,0.6667,,,,,,,
		
		int nclass_id=1;
		int gyear_id=9;
		int patent_id=0;
		int line_num = 0;
		
	    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
	    	if (line_num > 0) { // skip header
		        String line = value.toString();
		        String[] cols = line.split(","); // split to columns
		        if  (cols.length > 2) {
		        	Text pid = new Text(cols[patent_id]);
		        	Text nclass = new Text(cols[nclass_id]);
		        	output.collect(pid, nclass);
		        }
	    	}
	    	line_num++;
	    }

	}

	public static void main(String[] args) throws Exception {

	    Job job = new Job();
	    job.setJarByClass(Module4_Assign1.class);
	    job.setJobName("Module4_Assign1");
	    job.setNumReduceTasks(0);
	    
	    try{
	    	DistributedCache.addCacheFile(new URI("./list_of_classes.txt"), job.getConfiguration());
	    }catch(Exception e){
	    	System.out.println(e);
	    }
	    
	    job.setMapperClass(MyMapper1.class);
	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.waitForCompletion(true);
	    
	}
}