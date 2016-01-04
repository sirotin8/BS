package de.hshannover.vis.flink.jobdb;

import java.util.ArrayList;
import java.util.List;

import de.hshannover.vis.flink.jobdb.mapper.EmployeeMapper;
import de.hshannover.vis.flink.jobdb.utils.IParameter;
import de.hshannover.vis.flink.jobdb.utils.Loader;
import de.hshannover.vis.flink.jobdb.utils.OutputPath;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.core.fs.FileSystem.WriteMode;

public class A1DoubleName {

	@SuppressWarnings({ "unchecked", "serial" })
	public static void main(String[] args) throws Exception {

		List<IParameter> list = new ArrayList<IParameter>();
		list.add(new EmployeeMapper());
		list.add(new OutputPath("Output", ""));
		Loader loader = new Loader(list, args);

		loader.load();

		DataSet<Tuple4<String, String, String, Integer>> names = loader.data
				.get(0)
				.project(2, 1, 1)
				.types(String.class, String.class, String.class)
				.map(new MapFunction<Tuple3<String, String, String>, Tuple4<String, String, String, Integer>>() {
					public Tuple4<String, String, String, Integer> map(
							Tuple3<String, String, String> line)
							throws Exception {
						return new Tuple4<String, String, String, Integer>(
								line.f0, line.f1, line.f2, 1);
					}
				})

		;

		DataSet<Tuple3<String, String, String>> result = names
				.groupBy(0)
				.aggregate(Aggregations.MIN, 1)
				.and(Aggregations.MAX, 2)
				.and(Aggregations.SUM, 3)

				.filter(new FilterFunction<Tuple4<String, String, String, Integer>>() {
					public boolean filter(
							Tuple4<String, String, String, Integer> arg0)
							throws Exception {
						if (arg0.f3 > 1)
							return true;
						return false;
					}
				})

				.project(0, 1, 2)
				.types(String.class, String.class, String.class);

		// emit result
		result.writeAsCsv(loader.output.get(0).getPath(), "\n", " | ",
				WriteMode.OVERWRITE);

		// execute program
		final ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();
		env.execute(A1DoubleName.class.toString());
	}
}