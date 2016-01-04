package de.hshannover.vis.flink.jobdb.mapper;

import de.hshannover.vis.flink.jobdb.utils.IParameter;
import de.hshannover.vis.flink.jobdb.utils.JobReader;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.*;

public class EmployeeMapper
implements MapFunction<String, Tuple11
<Integer,String,String,String,String,String,String,Integer,Double,Integer,Integer>
>, IParameter {
	
//	"EMPLOYEE_ID"   "FIRST_NAME"    "LAST_NAME"     "EMAIL" "PHONE_NUMBER" 
//	"HIRE_DATE"     "JOB_ID"        "SALARY"        "COMMISSION_PCT"       
//	"MANAGER_ID"   "DEPARTMENT_ID"

	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public Tuple11
	<Integer,String,String,String,String,String,String,Integer,Double,Integer,Integer> 
	map(String line)
			throws Exception {
		
		String[] arr = JobReader.prepareString(line);
		return new Tuple11
				<Integer,String,String,String,String,String,String, 
				Integer,Double,Integer,Integer>
		(JobReader.getInteger(arr[0]),
						JobReader.getString(arr[1]),JobReader.getString(arr[2]),
						JobReader.getString(arr[3]),JobReader.getString(arr[4]),
						JobReader.getDate(arr[5]),
						JobReader.getString(arr[6]),
						JobReader.getInteger(arr[7]),JobReader.getDouble(arr[8]),
						JobReader.getInteger(arr[9]),JobReader.getInteger(arr[10])
						
						);
	}
	
	public String getParamName(){
		return "Employee";
	}

}
