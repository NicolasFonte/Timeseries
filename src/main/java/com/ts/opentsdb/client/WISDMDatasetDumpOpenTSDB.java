package com.ts.opentsdb.client;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class WISDMDatasetDumpOpenTSDB {

	/**
	 * Read WISDM dataset. For each line creates a metric wisdm.acc[XYZ] with
	 * tag status = '<jogging/walking/etc>' and user = 'userId'
	 * 
	 * @param args
	 * @throws Exception
	 */

	public static void main(String[] args) throws Exception {

		long initialTime = System.currentTimeMillis();

		String metricAccelX = "wisdm.acc.x";
		String metricAccelY = "wisdm.acc.y";
		String metricAccelZ = "wisdm.acc.z";

		System.out.println("Reading file");
		List<String> allLines = Files.readAllLines(Paths.get(
				"/Users/nicolasfontenele/development/workspace/nicolas/hbase/WISDM_ar_v1.1/WISDM_ar_v1.1_raw.txt"));

		for (String line : allLines) {
			try {

				DatapointWISDM dps = convertLineToDatapoint(line);

				if (dps != null) {

					dumpWISDMPost(metricAccelX, dps.getTime(), dps.getMetricX(), dps.getState(), dps.getUser());
					dumpWISDMPost(metricAccelY, dps.getTime(), dps.getMetricY(), dps.getState(), dps.getUser());
					dumpWISDMPost(metricAccelZ, dps.getTime(), dps.getMetricZ(), dps.getState(), dps.getUser());

				}

			} catch (NumberFormatException | ArrayIndexOutOfBoundsException ex) {
				continue;
			}
		}
		System.out.println(" Total time: " + (System.currentTimeMillis() - initialTime) / 1000);

	}

	private static DatapointWISDM convertLineToDatapoint(String line) {

		String[] atts = line.split(",");

		String userId = atts[0];
		String state = atts[1];

		String timeNano = atts[2];

		String xAcc = atts[3];
		String yAcc = atts[4];
		String zAcc = atts[5];

		if (zAcc.isEmpty()) {
			return null;
		}
		// forget end of line ;
		zAcc = zAcc.replace(";", "");

		return new DatapointWISDM(xAcc, yAcc, zAcc, timeNano, state, userId);

	}

	private static void dumpWISDMPost(String metric, long time, String value, String tagStatus, String tagUser) {

		try {
			String url = "http://localhost:5151/api/put";
			URL obj = new URL(url);
			HttpURLConnection con = (HttpURLConnection) obj.openConnection();

			// add request header
			con.setRequestMethod("POST");
			// con.setRequestProperty("User-Agent", USER_AGENT);
			con.setRequestProperty("Accept-Language", "en-US,en;q=0.5");

			String urlParameters = "{\"metric\":\"" + metric + "\",\"timestamp\":\"" + time + "\", \"value\": \""
					+ value + "\", \"tags\": { \"state\": \"" + tagStatus + "\", \"user\": \"" + tagUser + "\" }}";
			// Send post request
			con.setDoOutput(true);
			DataOutputStream wr = new DataOutputStream(con.getOutputStream());
			wr.writeBytes(urlParameters);
			wr.flush();
			wr.close();

			int responseCode = con.getResponseCode();
			System.out.println("\nSending 'POST' request to URL : " + url);
			System.out.println("Post parameters : " + urlParameters);
			System.out.println("Response Code : " + responseCode);

			BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
			String inputLine;
			StringBuffer response = new StringBuffer();

			while ((inputLine = in.readLine()) != null) {
				response.append(inputLine);
			}
			in.close();

			// print result
			System.out.println(response.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
