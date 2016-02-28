package com.ts.model;

import java.util.Map;

public interface HBaseTimeseries {

	Map<String, String> getProperties();

	Map<Long, String> getTimeseries();

}
