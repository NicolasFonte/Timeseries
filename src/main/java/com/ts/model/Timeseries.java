package com.ts.model;

import java.util.HashMap;
import java.util.Map;

/**
 * 
 * @author nicolasfontenele
 */
public class Timeseries implements HBaseTimeseries {
	
	private String objectId;
	
	private Map<Long, String> timeseries;
	private Map<String, String> properties;
	
	@Override
	public Map<Long, String> getTimeseries() {
		
		if ( timeseries == null ) {
			timeseries = new HashMap<>();
		}
		return timeseries;
	}
	
	@Override
	public Map<String, String> getProperties() {
		if ( properties == null ) {
			properties = new HashMap<>();
		}
		return properties;
	}
	
	public void setProperties(Map<String, String> properties) {
		this.properties = properties;
	}

	public String getObjectId() {
		return objectId;
	}

	public void setObjectId(String objectId) {
		this.objectId = objectId;
	}

	public void setTimeseries(Map<Long, String> timeseries) {
		this.timeseries = timeseries;
	}
}
