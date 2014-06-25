package com.siteify;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;



public class CustomRecordReader extends RecordReader<CustomKey,CustomValue> {

	private CustomKey key;
	private CustomValue value;
	private LineRecordReader reader = new LineRecordReader();
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		reader.close();
	}

	@Override
	public CustomKey getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return key;
	}

	@Override
	public CustomValue getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return reader.getProgress();
	}

	@Override
	public void initialize(InputSplit is, TaskAttemptContext tac)
			throws IOException, InterruptedException {
		reader.initialize(is, tac);
		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		boolean gotNextKeyValue = reader.nextKeyValue();
		if(gotNextKeyValue){
			if(key==null){
				key = new CustomKey();
			}
			if(value == null){
				value = new CustomValue();
			}
			Text line = reader.getCurrentValue();
			String[] tokens = line.toString().split("\t");
			key.setSensorType(new Text(tokens[0]));
			key.setTimestamp(new Text(tokens[1]));
			key.setStatus(new Text(tokens[2]));
			value.setValue1(new Text(tokens[3]));
			value.setValue2(new Text(tokens[4]));
		}
		else {
			key = null;
			value = null;
		}
		return gotNextKeyValue;
	}

}







