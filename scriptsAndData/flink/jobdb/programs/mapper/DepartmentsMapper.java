package de.hshannover.vis.flink.jobdb.mapper;

import de.hshannover.vis.flink.jobdb.utils.IParameter;
import de.hshannover.vis.flink.jobdb.utils.JobReader;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.*;

public class DepartmentsMapper implements MapFunction<String,
Tuple4<Integer,String,Integer,Integer>>, IParameter {
	
	/**
	 * "DEPARTMENT_ID" "DEPARTMENT_NAME"       "MANAGER_ID"    "LOCATION_ID"
	 */
	private static final long serialVersionUID = 1L;

	public Tuple4<Integer,String,Integer,Integer> map(String line)
			throws Exception {
		
		String[] arr = JobReader.prepareString(line);
		return new Tuple4<Integer,String,Integer,Integer>(JobReader.getInteger(arr[0]),
						JobReader.getString(arr[1]),
						JobReader.getInteger(arr[2]),JobReader.getInteger(arr[3])
						
						);
	}
	
	public String getParamName(){
		return "Departments";
	}

}
