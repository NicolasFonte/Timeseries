package com.ts.hbase.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.ts.exception.BackendException;
import com.ts.model.Timeseries;

/**
 * 
 * @author nicolasfontenele
 */
public class TSHBaseBackend extends HBaseBackend implements TimeseriesBackend {

	// row and family names should be as short as possible.
	private static String CF_TIMESERIES = "ts_cf";
	private static String TIMESERIES_TABLE = "ts_tbl";

	private static String CF_METADATA = "md_cf";

	public TSHBaseBackend() throws IOException {

		super();
		List<String> families = Arrays.asList(CF_TIMESERIES, CF_METADATA);
		createSchemaTables(getConfig(), TIMESERIES_TABLE, families);

	}

	@Override
	public Timeseries update(Timeseries entity) throws BackendException {
		createOrUpdate(entity);
		return entity;
	}

	@Override
	public void create(Timeseries entity) throws BackendException {
		createOrUpdate(entity);
	}

	private void createOrUpdate(Timeseries entity) {

		String id = entity.getObjectId();
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

				put.addColumn(Bytes.toBytes(CF_METADATA), propKey, propValue);
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

		Delete ts = new Delete(Bytes.toBytes(entity.getObjectId()));

		Table table = getTable(TIMESERIES_TABLE);

		try {
			table.delete(ts);
			closeConnection();
		} catch (IOException e) {
			e.printStackTrace();
		}
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
		ts.setObjectId(String.valueOf(identifier));

		Map<byte[], byte[]> timeByValue = result.getFamilyMap(Bytes.toBytes(CF_TIMESERIES));
		Map<byte[], byte[]> properties = result.getFamilyMap(Bytes.toBytes(CF_METADATA));

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

	public Timeseries getTimeSeriesByUserId(String id) {
		
		Timeseries ts = new Timeseries();

		Get getInterval = new Get(Bytes.toBytes(id));
		getInterval.addFamily(Bytes.toBytes(CF_TIMESERIES));
		Table tsTable = getTable(TIMESERIES_TABLE);

		try {
			Result result = tsTable.get(getInterval);

			NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(Bytes.toBytes(CF_TIMESERIES));
			ts.setTimeseries(convertFamilyToTs(familyMap));

		} catch (IOException e) {
			e.printStackTrace();
		}
		return ts;
	}

	@Override
	public List<Timeseries> list() throws BackendException {

		List<Timeseries> tsList = new ArrayList<Timeseries>();

		Scan scanAll = new Scan();
		Table tsTable = getTable(TIMESERIES_TABLE);

		ResultScanner scanner = null;
		try {
			scanner = tsTable.getScanner(scanAll);
			Iterator<Result> iterator = scanner.iterator();
			while (iterator.hasNext()) {
				Result next = iterator.next();
				Timeseries ts = new Timeseries();
				String objectId = Bytes.toString((next.getRow()));

				Map<byte[], byte[]> tsFamily = next.getFamilyMap(Bytes.toBytes(CF_TIMESERIES));
				Map<Long, String> tsMap = convertFamilyToTs(tsFamily);

				// TODO : map properties family
				// Map<byte[],byte[]> tsProperty =
				// next.getFamilyMap(Bytes.toBytes(CF_PROPERTIES));

				ts.setObjectId(objectId);
				ts.setTimeseries(tsMap);
				// ts.setProperties(properties);

				tsList.add(ts);

			}

		} catch (IOException e) {
			e.printStackTrace();
			throw new BackendException("Error get scanner: " + e.getMessage());
		} finally {
			if (scanner != null) {
				scanner.close();
			}
		}
		return tsList;
	}

	/**
	 * 
	 * @param id
	 * @param init
	 * @param end
	 * @return
	 */
	public Timeseries getFromTimeInterval(String id, long init, long end) {

		Timeseries ts = new Timeseries();
		Map<Long, String> tsValues = new HashMap<Long, String>();

		Get getInterval = new Get(Bytes.toBytes(id));
		getInterval.addFamily(Bytes.toBytes(CF_TIMESERIES));
		Table tsTable = getTable(TIMESERIES_TABLE);

		try {
			Result result = tsTable.get(getInterval);

			NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(Bytes.toBytes(CF_TIMESERIES));

			for (Entry<byte[], byte[]> keyValue : familyMap.entrySet()) {

				long key = Bytes.toLong(keyValue.getKey());

				if (key < init) {
					continue;
				}

				if (key > end) {
					break;
				}

				tsValues.put(key, Bytes.toString(keyValue.getValue()));
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
		return ts;
	}

	protected Map<Long, String> convertFamilyToTs(Map<byte[], byte[]> tsFamily) {

		Map<Long, String> ts = new HashMap<Long, String>();
		if (tsFamily == null || tsFamily.isEmpty()) {
			return ts;
		}

		for (Entry<byte[], byte[]> tsBytes : tsFamily.entrySet()) {
			long time = Bytes.toLong(tsBytes.getKey());
			String value = Bytes.toString(tsBytes.getValue());
			ts.put(time, value);
		}
		return ts;
	}
}
