package de.hshannover.vis.flink.jobdb;

import java.util.ArrayList;
import java.util.List;

import de.hshannover.vis.flink.jobdb.mapper.EmployeeMapper;
import de.hshannover.vis.flink.jobdb.utils.IParameter;
import de.hshannover.vis.flink.jobdb.utils.Joiner;
import de.hshannover.vis.flink.jobdb.utils.Joiner.Mode;
import de.hshannover.vis.flink.jobdb.utils.Loader;
import de.hshannover.vis.flink.jobdb.utils.OutputPath;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.core.fs.FileSystem.WriteMode;

@SuppressWarnings({ "unchecked", "serial" })
public class A3NameWithBoss {

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
		DataSet<Tuple3<String, Integer, Integer>> names = loader.data
				.get(0)
				.project(1, 2, 0, 9)
				.types(String.class, String.class, Integer.class, Integer.class)
				.map(new MapFunction<Tuple4<String, String, Integer, Integer>, Tuple3<String, Integer, Integer>>() {
					public Tuple3<String, Integer, Integer> map(
							Tuple4<String, String, Integer, Integer> line)
							throws Exception {
						return new Tuple3<String, Integer, Integer>(line.f0
								+ " " + line.f1, line.f2, line.f3);
					}

				});

		Tuple3<String, Integer, Integer> myNull = new Tuple3<String, Integer, Integer>(
				"-1", -1, -1);
		
		DataSet<Tuple6<String, Integer, Integer,String, Integer, Integer>>
		leftNames = names.project(0,1,2,0,1,2).types(String.class,Integer.class
				,Integer.class,String.class,Integer.class,Integer.class);

		@SuppressWarnings("rawtypes")
		DataSet result = leftNames
				.coGroup(names)
				.where(2)
				// boss-key
				.equalTo(1)
				// empl-key
				.with(new Joiner<Tuple6<String, Integer, Integer,String,
						Integer, Integer>, Tuple3<String, Integer, Integer>>(
						Mode.LEFT, myNull))
				.project(0,3).types(String.class,String.class)
				
		;

		// emit result
		result.writeAsCsv(loader.output.get(0).getPath(), "\n", " | ",
				WriteMode.OVERWRITE);
//		leftNames.writeAsCsv(loader.output.get(0).getPath()+"3", "\n", " | ");
//		names.writeAsCsv(loader.output.get(0).getPath()+"2", "\n", " | ");

		// execute program
		env.execute(A3NameWithBoss.class.toString());
	}
}