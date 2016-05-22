package com.ts.opentsdb.client;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;

import org.apache.commons.io.IOUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class OpenTSDBClientAPISamples {

	public static void main(String args[]) {

		// printStats();
		// printAggregators();
		querySample();
	}

	private static void printStats() {
		String statsService = "http://localhost:5151/api/stats";
		JSONArray stats = null;

		try {
			URL url = new URL(statsService);
			stats = new JSONArray(IOUtils.toString(url, Charset.forName("UTF-8")));
		} catch (IOException | JSONException ex) {
			System.out.println("Error Opentsdb client");
		}
		System.out.println("Printing All statistics");
		System.out.println(stats.toString());
	}

	private static void printAggregators() {
		String statsService = "http://localhost:5151/api/aggregators";
		JSONArray stats = null;

		try {
			URL url = new URL(statsService);
			stats = new JSONArray(IOUtils.toString(url, Charset.forName("UTF-8")));
		} catch (IOException | JSONException ex) {
			System.out.println("Error Opentsdb client");
		}
		System.out.println("Printing All Aggregators");
		System.out.println(stats.toString());
	}

	private static void querySample() {
		String statsService = "http://localhost:5151/api/query?start=1297574486&end=1297574501&m=sum:rate:proc.stat.cpu{host=foo,type=user}";
		JSONArray stats = null;

		try {
			URL url = new URL(statsService);
			stats = new JSONArray(IOUtils.toString(url, Charset.forName("UTF-8")));
		} catch (IOException | JSONException ex) {
			System.out.println("Error Opentsdb client");
		}
		System.out.println("Query by timestamp");
		System.out.println(stats.toString());

	}

}
