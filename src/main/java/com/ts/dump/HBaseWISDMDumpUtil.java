package com.ts.dump;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ts.exception.BackendException;
import com.ts.hbase.dao.TimeseriesBackend;
import com.ts.hbase.dao.TSHBaseBackend;
import com.ts.model.Timeseries;

public class HBaseWISDMDumpUtil {

	public static void main(String[] args) throws Exception {

		System.out.println("Reading file");
		List<String> allLines = Files.readAllLines(Paths.get(
				"/Users/nicolasfontenele/development/workspace/nicolas/hbase/WISDM_ar_v1.1/WISDM_ar_v1.1_raw_reduced.txt"));
		System.out.println("Creating time series objects");
		Map<String, Map<Long, String>> objectTs = buildTimeseries(allLines);
		System.out.println("Importing into HBase");
		long totalHBaseTime = importHBase(objectTs);
		System.out.println("HBase import took: " + totalHBaseTime / 1000 + " seconds");
		
	}

	private static long importHBase(Map<String, Map<Long, String>> objectTs) throws IOException, BackendException {

		TimeseriesBackend hbase = new TSHBaseBackend();
		long startHBaseDump = System.currentTimeMillis();
		for (String id : objectTs.keySet()) {
			System.out.println("importing object: " + id);
			Timeseries ts = new Timeseries();
			ts.setObjectId(id);
			ts.setTimeseries(objectTs.get(id));

			hbase.create(ts);
		}
		long totalHBaseTime  = System.currentTimeMillis() - startHBaseDump;
		return totalHBaseTime;
	}

	private static Map<String, Map<Long, String>> buildTimeseries(List<String> allLines) {
		// Lets just create a map with objectid:ts:value to fit our model

		Map<String, Map<Long, String>> objectTs = new HashMap<String, Map<Long, String>>();
		Map<Long, String> tsValue = new HashMap<Long, String>();
		for (String line : allLines) {
			try {
			// Java long is millsec - Dataset is nano sec
			String[] atts = line.split(",");

			String userId = atts[0];
			String state = atts[1];

			Long timeNano = Long.valueOf(atts[2]);

			String xAcc = atts[3];
			String yAcc = atts[4];
			String zAcc = atts[5];
			
			if ( zAcc.isEmpty() ) {
				continue;
			}
			// forget end of line ;
			zAcc = zAcc.replace(";", "");
			// Lets save values as - state:x,y,z
			String value = state + ":" + xAcc + "," + yAcc + "," + zAcc;

			tsValue.put(timeNano, value);
			objectTs.put(userId, tsValue);
			} catch ( NumberFormatException | ArrayIndexOutOfBoundsException ex ) {
				// forget about unformatted lines
				continue;
			}
		}
		return objectTs;
	}
}
