package com.ts.hbase.dao;

import com.ts.model.Timeseries;

public interface TimeseriesBackend extends CRUDBackend<Timeseries> {
	
	Timeseries getFromTimeInterval(String id, long init, long end);
	Timeseries getTimeSeriesByUserId(String id);
}
