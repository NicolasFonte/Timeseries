package com.ts.simpletsdb;

import java.io.IOException;
import java.util.List;

import com.ts.exception.BackendException;
import com.ts.hbase.dao.TSHBaseBackend;
import com.ts.hbase.dao.TimeseriesBackend;
import com.ts.model.Timeseries;

/**
 * Wrapper class for SimpleTSDB features.
 * Make use of backend HBASE classes.
 * 
 * 
 * @author nicolasfontenele
 *
 */
public class SimpleTSDB {

	private TimeseriesBackend hbase;
	
	public SimpleTSDB() throws IOException {
		hbase = new TSHBaseBackend();
	}
	
	public Timeseries update(Timeseries entity) throws BackendException {
		return hbase.update(entity);
	}

	public void create(Timeseries entity) throws BackendException {
		hbase.create(entity);
	}

	public void remove(Timeseries entity) throws BackendException {
		hbase.remove(entity);
	}

	public Timeseries read(String identifier) throws BackendException {
		return hbase.read(identifier);
	}

	/**
	 * Get a time series from a given object id.
	 * @param id
	 * @return
	 */
	public Timeseries getTimeSeriesByUserId(String id) {
		return hbase.getTimeSeriesByUserId(id);
	}

	/**
	 * Get a list of all timeseries found under the database.
	 * 
	 * @return
	 * @throws BackendException
	 */
	public List<Timeseries> list() throws BackendException {
		return hbase.list();
	}

	/**
	 * Get time series from interval giving object id, init time and end time.
	 * Times in milliseconds.
	 */
	public Timeseries getFromTimeInterval(String id, long init, long end) {
		return hbase.getFromTimeInterval(id, init, end);
	}

}
