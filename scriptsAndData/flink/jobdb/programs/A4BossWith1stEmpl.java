package de.hshannover.vis.flink.jobdb;

import java.util.ArrayList;
import java.util.List;

import de.hshannover.vis.flink.jobdb.mapper.EmployeeMapper;
import de.hshannover.vis.flink.jobdb.utils.IParameter;
import de.hshannover.vis.flink.jobdb.utils.Loader;
import de.hshannover.vis.flink.jobdb.utils.OutputPath;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.core.fs.FileSystem.WriteMode;

public class A4BossWith1stEmpl {

	@SuppressWarnings({ "unchecked", "serial", "rawtypes" })
	public static void main(String[] args) throws Exception {

		List<IParameter> list = new ArrayList<IParameter>();
		list.add(new EmployeeMapper());
		list.add(new OutputPath("Output", ""));
		Loader loader = new Loader(list, args);

		loader.load();

		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		// get input data
		DataSet<Tuple4<String,String, Integer, Integer>> names = loader.data.get(0)
				.project(1,2,5,0,9)
				.types(String.class,String.class,String.class,Integer.class,Integer.class)
				
				.map(new MapFunction<Tuple5<String,String,String,Integer,Integer>, 
						Tuple4<String,String, Integer, Integer>>() {
					public Tuple4<String,String, Integer, Integer> map(
							Tuple5<String,String,String,Integer,Integer> line)
							throws Exception {
						return new Tuple4<String,String, Integer, Integer>(
								line.f0 + " " + line.f1,
								line.f2.toString(), line.f3,line.f4);
					}

				})
				;
		
		DataSet<Tuple2<String,Integer>> empl = names
				.project(1,3).types(String.class,Integer.class)
				.groupBy(1).aggregate(Aggregations.MIN, 0)
				
				;
		
		


//		DataSet<Tuple3<String,Integer,Integer>> myNull = env
//				.fromElements(new Tuple3<String,Integer,Integer>("-1",-1,-1));
		
//		names = names.union(myNull);
		
		DataSet result = names.join(empl).where(2).equalTo(1)
				.projectFirst(0,1).projectSecond(0)
				.types(String.class,String.class,String.class);

			// emit result
			result.writeAsCsv(loader.output.get(0).getPath(), "\n", " | ",
					WriteMode.OVERWRITE);

			// execute program
			env.execute(A4BossWith1stEmpl.class.toString());
	}
}