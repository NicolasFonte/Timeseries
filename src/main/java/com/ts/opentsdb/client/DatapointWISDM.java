package com.ts.opentsdb.client;

import java.util.concurrent.TimeUnit;

public class DatapointWISDM {

	private String metricX;
	private String metricY;
	private String metricZ;

	private Long time;

	private String state;
	private String user;

	public DatapointWISDM(String metricX, String metricY, String metricZ, String time, String state, String user) {

		this.metricX = metricX;
		this.metricY = metricY;
		this.metricZ = metricZ;

		long timestamp = Long.valueOf(time);
		
		this.time = TimeUnit.NANOSECONDS.toMillis(timestamp);
		this.state = state;
		this.user = user;

	}

	public String getMetricX() {
		return metricX;
	}

	public void setMetricX(String metricX) {
		this.metricX = metricX;
	}

	public String getMetricY() {
		return metricY;
	}

	public void setMetricY(String metricY) {
		this.metricY = metricY;
	}

	public String getMetricZ() {
		return metricZ;
	}

	public void setMetricZ(String metricZ) {
		this.metricZ = metricZ;
	}

	public Long getTime() {
		return time;
	}

	public void setTime(Long time) {
		this.time = time;
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

}
