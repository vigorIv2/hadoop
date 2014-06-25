package com.siteify;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;

public class ReduceSideJoin2 {

	private static final String TAB = "\t";

	public static class PatentClassMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

		private Text outKey = new Text();
		private Text outValue = new Text();

		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String[] tokens = line.split(",");
			String patentStr = tokens[0];
			String patentAssigneeId = tokens[10];
			outKey.set(patentAssigneeId);
			outValue.set("patent" + TAB + patentStr);
			System.out.println("MP-Patent assignee " + patentAssigneeId + " Patent " + patentStr);
			output.collect(outKey, outValue);

		}
	}

	public static class AssigneeMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		private Text outKey = new Text();
		private Text outValue = new Text();

		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String[] tokens = line.split(",");
			String assigneeId = Integer.parseInt(tokens[0]) + "";
			String assigneeStr = tokens[1];
			outKey.set(assigneeId);
			outValue.set("assignee" + TAB + assigneeStr);
			System.out.println("MC-Patent assignee " + assigneeStr + " ID " + assigneeId);
			output.collect(outKey, outValue);
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

		private Text outKey = new Text();
		private Text outValue = new Text();

		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			List<String> patentValues = new ArrayList<String>();
			while (values.hasNext()) {
				String value = values.next().toString();
				String[] split = value.split(TAB);
				if (split[0].equals("assignee")) {
					System.out.println("R-assignee " + split[1]);
					outKey.set(split[1]);

				} else {
					System.out.println("R-patent " + split[1]);
					patentValues.add(split[1]);
				}
			}
			for (String patent : patentValues) {
				outValue.set(patent);
				output.collect(outKey, outValue);
			}

		}
	}

	public static void main(String[] args) throws Exception {
		
		JobConf conf = new JobConf(ReduceSideJoin2.class);
		conf.setJobName("ReduceSideJoin2");

		conf.setReducerClass(Reduce.class);

		MultipleInputs.addInputPath(conf, new Path(args[0]), TextInputFormat.class, PatentClassMap.class);
		MultipleInputs.addInputPath(conf, new Path(args[1]), TextInputFormat.class, AssigneeMap.class);
		FileOutputFormat.setOutputPath(conf, new Path(args[2]));

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setReducerClass(Reduce.class);
		conf.setOutputValueClass(Text.class);
		conf.setOutputKeyClass(Text.class);

		JobClient.runJob(conf);
	}
}