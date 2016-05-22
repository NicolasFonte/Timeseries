package com.ts.hbase.dao;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;

/**
 * @author nicolasfontenele
 *
 *         TODO : Exceptionss
 */
public class HBaseBackend {

	private Configuration config;
	private Connection connection;

	public HBaseBackend() {

		config = HBaseConfiguration.create();
		getConfig().addResource(new Path("/Users/nicolasfontenele/tools/apps/hbase/conf", "hbase-site.xml"));
		getConfig().addResource(new Path("/Users/nicolasfontenele/tools/apps/hadoop/etc/hadoop", "core-site.xml"));

	}

	public void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {

		if (!admin.tableExists(table.getTableName())) {
			// admin.disableTable(table.getTableName());
			// admin.deleteTable(table.getTableName());
			admin.createTable(table);
		}
		// admin.createTable(table);
	}

	protected Table getTable(String tableName) {

		Table table = null;
		try  {
			startConnection();
			table = connection.getTable(TableName.valueOf(tableName));
		} catch (IOException ex) {
			ex.printStackTrace();
		}

		return table;
	}
	
	protected void startConnection() throws IOException {
		
		if ( connection == null || connection.isClosed()    ) {
			connection = ConnectionFactory.createConnection(config);
		}
	}
	
	protected void closeConnection() throws IOException {
		
		if ( connection != null && !connection.isClosed() ) {
			connection.close();
		}
	}
	

	public void createSchemaTables(Configuration config, String tableName, List<String> columnFamilies)
			throws IOException {

		try (Connection connection = ConnectionFactory.createConnection(config); Admin admin = connection.getAdmin()) {

			HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));

			for (String cf : columnFamilies) {
				table.addFamily(new HColumnDescriptor(cf).setCompressionType(Algorithm.GZ));
			}

			System.out.print("Creating table :  " + tableName);
			createOrOverwrite(admin, table);
			System.out.println(" Done.");
		} catch (IOException ex) {
			ex.printStackTrace();
			throw new IOException(ex);
		}
	}

	public static void modifySchema(Configuration config) throws IOException {
		try (Connection connection = ConnectionFactory.createConnection(config); Admin admin = connection.getAdmin()) {

			// TODO : organize schema for modifying

			// TableName tableName = TableName.valueOf(TABLE_NAME);
			// if (admin.tableExists(tableName)) {
			// System.out.println("Table does not exist.");
			// System.exit(-1);
			// }

			// HTableDescriptor table = new HTableDescriptor(tableName);
			//
			// // Update existing table
			// HColumnDescriptor newColumn = new HColumnDescriptor("NEWCF");
			// newColumn.setCompactionCompressionType(Algorithm.GZ);
			// newColumn.setMaxVersions(HConstants.ALL_VERSIONS);
			// admin.addColumn(tableName, newColumn);

			// // Update existing column family
			// HColumnDescriptor existingColumn = new
			// HColumnDescriptor(CF_DEFAULT);
			// existingColumn.setCompactionCompressionType(Algorithm.GZ);
			// existingColumn.setMaxVersions(HConstants.ALL_VERSIONS);
			// table.modifyFamily(existingColumn);
			// admin.modifyTable(tableName, table);
			//
			// // Disable an existing table
			// admin.disableTable(tableName);
			//
			// // Delete an existing column family
			// admin.deleteColumn(tableName, CF_DEFAULT.getBytes("UTF-8"));
			//
			// // Delete a table (Need to be disabled first)
			// admin.deleteTable(tableName);
		}
	}

	public Configuration getConfig() {
		return config;
	}

}