package de.hshannover.vis.flink.jobdb.mapper;

import de.hshannover.vis.flink.jobdb.utils.IParameter;
import de.hshannover.vis.flink.jobdb.utils.JobReader;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.*;

public class LocationsMapper
		implements
		MapFunction<String, Tuple6<Integer, String, String, String, String, String>>,
		IParameter {

	/**
	 * "LOCATION_ID"   "STREET_ADDRESS"        "POSTAL_CODE"   "CITY"  "STATE_PROVINCE""COUNTRY_ID"
	 */
	private static final long serialVersionUID = 1L;

	public Tuple6<Integer, String, String, String, String, String> map(
			String line) throws Exception {

		String[] arr = JobReader.prepareString(line);
		return new Tuple6<Integer, String, String, String, String, String>(
				JobReader.getInteger(arr[0]), JobReader.getString(arr[1]),
				JobReader.getString(arr[2]), JobReader.getString(arr[3]),
				JobReader.getString(arr[4]), JobReader.getString(arr[5])

		);
	}

	public String getParamName() {
		return "Locations";
	}

}
