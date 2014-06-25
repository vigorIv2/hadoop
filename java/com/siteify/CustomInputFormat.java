package com.siteify;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;





public class CustomInputFormat extends FileInputFormat<CustomKey,CustomValue> {
	
	
	@Override
	public RecordReader<CustomKey,CustomValue> createRecordReader(InputSplit arg0,
			TaskAttemptContext arg1) throws IOException, InterruptedException {
		return new CustomRecordReader();
	}	
}