package com.datastax.spark.example;

import java.util.Date;

public class SensorReading implements java.io.Serializable {
	private static final long serialVersionUID = 1L;
	private String id;
	private int value;
	private Date time;
	
	public SensorReading(String id, int value, Date time) {
		super();
		this.id = id;
		this.value = value;
		this.time = time;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public int getValue() {
		return value;
	}
	public void setValue(int value) {
		this.value = value;
	}
	public Date getTime() {
		return time;
	}
	public void setTime(Date time) {
		this.time = time;
	}
	@Override
	public String toString() {
		return "SensorReading [id=" + id + ", value=" + value + ", time=" + time + "]";
	}
	
	
}