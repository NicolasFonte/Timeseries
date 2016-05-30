package com.ts.model;

import java.util.Map;

public interface Timeseries {

	Map<String, String> getProperties();

	Map<Long, String> getTimeseries();
	
	String getObjectId();

}
