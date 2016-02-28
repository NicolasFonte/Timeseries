package com.ts.converter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ts.hbase.dao.TimeseriesBackend;
import com.ts.hbase.dao.TsHBaseBackend;
import com.ts.model.Timeseries;

public class HBaseWISDMDumpUtil {

	public static void main(String[] args) throws IOException, Exception {

		TimeseriesBackend hbase = new TsHBaseBackend();
		
		List<String> allLines = Files.readAllLines(Paths.get(
				"/Users/nicolasfontenele/development/workspace/nicolas/hbase/WISDM_ar_v1.1/WISDM_ar_v1.1_raw.txt"));

		// Lets just create a map with objectid:ts:value to fit our model

		Map<Integer, Map<Long, String>> objectTs = new HashMap<Integer, Map<Long, String>>();
		Map<Long, String> tsValue = new HashMap<Long, String>();
		for (String line : allLines) {
			try {
			// Java long is millsec - Dataset is nano sec
			String[] atts = line.split(",");

			Integer userId = Integer.valueOf(atts[0]);
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

		for (Integer id : objectTs.keySet()) {

			Timeseries ts = new Timeseries();
			ts.setObjectId(id);
			ts.setTimeseries(objectTs.get(id));

			hbase.create(ts);

		}
	}
}
