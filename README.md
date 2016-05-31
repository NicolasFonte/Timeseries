# Timeseries
Project for storing and manipulating Time Series Data using HBase.

API Usage:

		// Create simple tsdb normally - default hbase port.
		SimpleTSDB simpleTsdb = new SimpleTSDB();
		
		// sample timeseries object
		MovementTimeseries ts = new MovementTimeseries();
		
		// use operations
		simpleTsdb.getTimeSeriesByUserId("sample-object");
		simpleTsdb.getFromTimeInterval("sample-object", System.currentTimeMillis() - 500L, System.currentTimeMillis() );
		simpleTsdb.create(ts);

Timeseries custom classe sample:

public class MovementTimeseries implements Timeseries {

	public Map<String, String> getProperties() {
		// Do not consider properties
		return null;
	}
	
	public Map<Long, String> getTimeseries() {

		// Sample timeseries.
		Map<Long, String > ts = new HashMap<Long, String>();
		ts.put(System.currentTimeMillis(), "45.5");
		ts.put(System.currentTimeMillis() + 10L, "35.5");
		ts.put(System.currentTimeMillis() + 20L, "65.5");
		
		return ts;
		
	}

	@Override
	public String getObjectId() {
		return "sample-object";
	}
	
}
