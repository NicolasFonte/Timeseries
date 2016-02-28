package com.ts.model;

import java.util.HashMap;
import java.util.Map;

/**
 * 
 * 
 * @author nicolasfontenele
 *
 * TODO : make it generic later. Its to much 'HBase oriented'. Should be a generic POJO
 * TODO : add start/end time
 */
public class Timeseries implements HBaseTimeseries {
	
	private int objectId;
	
	private Map<Long, String> timeseries;
	private Map<String, String> properties;
	
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

	public int getObjectId() {
		return objectId;
	}

	public void setObjectId(int objectId) {
		this.objectId = objectId;
	}

	@Override
	public Map<Long, String> getTimeseries() {
		
		if ( timeseries == null ) {
			timeseries = new HashMap<>();
		}
		return timeseries;
	}

	public void setTimeseries(Map<Long, String> timeseries) {
		this.timeseries = timeseries;
	}
}
