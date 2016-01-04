package de.hshannover.vis.flink.jobdb;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.core.fs.FileSystem.WriteMode;

import de.hshannover.vis.flink.jobdb.mapper.DepartmentsMapper;
import de.hshannover.vis.flink.jobdb.mapper.EmployeeMapper;
import de.hshannover.vis.flink.jobdb.mapper.LocationsMapper;
import de.hshannover.vis.flink.jobdb.utils.IParameter;
import de.hshannover.vis.flink.jobdb.utils.Loader;
import de.hshannover.vis.flink.jobdb.utils.OutputPath;

public class A8DepInfo {

	@SuppressWarnings({ "unchecked", "rawtypes", "serial" })
	public static void main(String[] args) throws Exception {

		List<IParameter> list = new ArrayList<IParameter>();
		list.add(new DepartmentsMapper());
		list.add(new LocationsMapper());
		list.add(new EmployeeMapper());
		list.add(new OutputPath("Output", ""));
		Loader loader = new Loader(list, args);

		loader.load();

		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		// get input data
		DataSet<Tuple3<Integer,String,Integer>> dep = loader.data.get(0)
				.project(0,1,3).types(Integer.class,String.class,Integer.class)
				;

		DataSet<Tuple2<Integer,String>> loc = loader.data.get(1)
				.project(0,5).types(Integer.class,String.class)
				;
		
		DataSet<Tuple3<Integer,String,String>> depLoc = dep.join(loc).where(2).equalTo(0)
				.projectFirst(0,1).projectSecond(1)
				.types(Integer.class,String.class,String.class)
				;

		DataSet empl = loader.data.get(2)
				.project(10,7).types(Integer.class,Integer.class)
				.map(new MapFunction<Tuple2<Integer,Integer>,
						Tuple3<Integer,Integer,Integer>>(){
							@Override
							public Tuple3<Integer, Integer, Integer> map(
									Tuple2<Integer, Integer> in)
									throws Exception {
								return new Tuple3<Integer,Integer,Integer>(
										in.f0,in.f1,1);
							}
				})
				.groupBy(0).aggregate(Aggregations.SUM, 1)
				.and(Aggregations.SUM, 2)
				;
		
		DataSet result = depLoc.join(empl).where(0).equalTo(0)
				.projectFirst(2,1).projectSecond(1,2)
				.types(String.class,String.class,Integer.class,Integer.class)
				;
		
		// emit result
		result.writeAsCsv(loader.output.get(0).getPath(), "\n", " | ",
				WriteMode.OVERWRITE);

		// execute program
		env.execute(A8DepInfo.class.toString());
	}
}