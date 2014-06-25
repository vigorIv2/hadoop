package com.siteify;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class CustomKey implements WritableComparable{
	private Text SensorType,timestamp,status;
	
	public CustomKey(){
		this.SensorType = new Text();
		this.timestamp = new Text();
		this.status = new Text();
	}
	public CustomKey(Text SensorType,Text timestamp,Text status){
		this.SensorType = SensorType;
		this.timestamp = timestamp;
		this.status = status;		
	}
	public void readFields(DataInput in) throws IOException{
		SensorType.readFields(in);
		timestamp.readFields(in);
		status.readFields(in);
	}
	
	public void write(DataOutput out) throws IOException{
		SensorType.write(out);
		timestamp.write(out);
		status.write(out);
	}
	public int compareTo(Object o){
		CustomKey other = (CustomKey)o;
		int cmp = SensorType.compareTo(other.SensorType);
		if(cmp != 0){
				return cmp;
		}
		cmp = timestamp.compareTo(other.timestamp);
		if(cmp != 0){
				return cmp;
		}
		return status.compareTo(other.status);
		
	}
	public Text getSensorType() {
		return SensorType;
	}
	public void setSensorType(Text sensorType) {
		SensorType = sensorType;
	}
	public Text getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(Text timestamp) {
		this.timestamp = timestamp;
	}
	public Text getStatus() {
		return status;
	}
	public void setStatus(Text status) {
		this.status = status;
	}
	

}