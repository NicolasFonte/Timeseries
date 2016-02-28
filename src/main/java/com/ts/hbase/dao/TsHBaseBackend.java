package com.ts.hbase.dao;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.ts.exception.BackendException;
import com.ts.model.Timeseries;

public class TsHBaseBackend extends HBaseBackend implements TimeseriesBackend {

	// row and family names should be as short as possible.

	private static String CF_TIMESERIES = "ts_cf";
	private static String CF_PROPERTIES = "prop_cf";
	private static String TIMESERIES_TABLE = "ts_tbl";

	public TsHBaseBackend() throws IOException {

		super();
		List<String> families = Arrays.asList(CF_TIMESERIES, CF_PROPERTIES);
		createSchemaTables(getConfig(), TIMESERIES_TABLE, families);

	}

	@Override
	public Timeseries update(Timeseries entity) throws BackendException {
		return null;
	}

	@Override
	public void create(Timeseries entity) throws BackendException {

		int id = entity.getObjectId();
		Map<Long, String> ts = entity.getTimeseries();
		Map<String, String> properties = entity.getProperties();

		Put put = new Put(Bytes.toBytes(id));

		for (Entry<Long, String> timeByValue : ts.entrySet()) {

			byte[] time = Bytes.toBytes(timeByValue.getKey());
			byte[] value = Bytes.toBytes(timeByValue.getValue());

			put.addColumn(Bytes.toBytes(CF_TIMESERIES), time, value);
		}

		if (!properties.isEmpty()) {
			for (Entry<String, String> timeByValue : properties.entrySet()) {

				byte[] propKey = Bytes.toBytes(timeByValue.getKey());
				byte[] propValue = Bytes.toBytes(timeByValue.getValue());

				put.addColumn(Bytes.toBytes(CF_PROPERTIES), propKey, propValue);
			}
		}

		Table table = getTable(TIMESERIES_TABLE);

		try {
			table.put(put);
			closeConnection();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void remove(Timeseries entity) throws BackendException {

	}

	@Override
	public Timeseries read(String identifier) throws BackendException {

		Get get = new Get(Bytes.toBytes(identifier));

		Result result = null;
		try {
			result = getTable(TIMESERIES_TABLE).get(get);
		} catch (IOException e) {
			e.printStackTrace();
		}

		Timeseries ts = new Timeseries();
		ts.setObjectId(Integer.valueOf(identifier));

		Map<byte[], byte[]> timeByValue = result.getFamilyMap(Bytes.toBytes(CF_TIMESERIES));
		Map<byte[], byte[]> properties = result.getFamilyMap(Bytes.toBytes(CF_PROPERTIES));

		Map<Long, String> entityTs = new HashMap<>();
		for (Entry<byte[], byte[]> tsFamily : timeByValue.entrySet()) {

			entityTs.put(Bytes.toLong(tsFamily.getKey()), Bytes.toString(tsFamily.getValue()));
		}

		ts.setTimeseries(entityTs);

		Map<String, String> entityProp = new HashMap<>();
		for (Entry<byte[], byte[]> propFamily : properties.entrySet()) {
			entityProp.put(Bytes.toString(propFamily.getKey()), Bytes.toString(propFamily.getValue()));
		}

		ts.setProperties(entityProp);

		return ts;

	}

	@Override
	public List<Timeseries> list() throws BackendException {
		// TODO Auto-generated method stub
		return null;
	}

}
