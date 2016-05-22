package com.ts.dump;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class MySQLWISDMDUMPUtil {
	public static void main(String[] args) throws IOException {

		System.out.println("Reading file");
		List<String> allLines = Files.readAllLines(Paths.get(
				"/Users/nicolasfontenele/development/workspace/nicolas/hbase/WISDM_ar_v1.1/WISDM_ar_v1.1_raw.txt"));
		System.out.println("Creating time series objects");
		List<TimeseriesMysqlVo> objectTs = buildTimeseriesMysql(allLines);
		System.out.println("Importing into Mysql");
		long totalMysqlTime = importMysql(objectTs);
		System.out.println("Mysql import took: " + totalMysqlTime / 1000 + " seconds");

	}

	private static long importMysql(List<TimeseriesMysqlVo> objectTs) {

		long startTime = System.currentTimeMillis();

		String dbURL = "jdbc:mysql://localhost:3306/wisdm";
		String username = "root";
		String password = "";

		try (Connection conn = DriverManager.getConnection(dbURL, username, password)) {

			String sql = "INSERT INTO timeseries (id, timeseries, value) VALUES (?, ?, ?)";
			PreparedStatement statement = conn.prepareStatement(sql);
			int count = 0;
			// int batchSize = 1000;
			for (TimeseriesMysqlVo vo : objectTs) {

				statement.setString(1, vo.id);
				statement.setString(2, vo.timeseries);
				statement.setString(3, vo.value);
				System.out.println("Added batch: " + count++);
				statement.addBatch();

				// int rowsInserted = statement.executeUpdate();
//				if (rowsInserted > 0) {
//					System.out.println("A new user was inserted successfully!");
//				}
			}
			System.out.println("starting batch of inserts");
			statement.executeBatch();
			System.out.println("finished batch of inserts");

		} catch (SQLException ex) {
			ex.printStackTrace();
		}

		long endTime = System.currentTimeMillis();

		return (endTime - startTime);
	}

	private static List<TimeseriesMysqlVo> buildTimeseriesMysql(List<String> allLines) {
		// Lets just create a map with objectid:ts:value to fit our model

		List<TimeseriesMysqlVo> rows = new ArrayList<TimeseriesMysqlVo>();
		
		for (String line : allLines) {
			try {
				// Java long is millsec - Dataset is nano sec
				
				TimeseriesMysqlVo vo = new TimeseriesMysqlVo();
				
				String[] atts = line.split(",");

				String userId = atts[0];
				String state = atts[1];

				String timeNano = atts[2];

				String xAcc = atts[3];
				String yAcc = atts[4];
				String zAcc = atts[5];

				if (zAcc.isEmpty()) {
					continue;
				}
				// forget end of line ;
				zAcc = zAcc.replace(";", "");
				// Lets save values as - state:x,y,z
				String value = state + ":" + xAcc + "," + yAcc + "," + zAcc;
				
				vo.value = value;
				vo.id = userId;
				vo.timeseries = timeNano;
				
				rows.add(vo);
				

			} catch (NumberFormatException | ArrayIndexOutOfBoundsException ex) {
				// forget about unformatted lines
				continue;
			}
		}
		return rows;
	}
	
	
	
}
