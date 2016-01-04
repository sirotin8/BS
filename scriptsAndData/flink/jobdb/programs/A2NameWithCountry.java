package de.hshannover.vis.flink.jobdb;

import java.util.ArrayList;
import java.util.List;

import de.hshannover.vis.flink.jobdb.mapper.DepartmentsMapper;
import de.hshannover.vis.flink.jobdb.mapper.EmployeeMapper;
import de.hshannover.vis.flink.jobdb.mapper.LocationsMapper;
import de.hshannover.vis.flink.jobdb.utils.IParameter;
import de.hshannover.vis.flink.jobdb.utils.Loader;
import de.hshannover.vis.flink.jobdb.utils.OutputPath;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.core.fs.FileSystem.WriteMode;

public class A2NameWithCountry {

	@SuppressWarnings({ "unchecked", "serial", "rawtypes" })
	public static void main(String[] args) throws Exception {

		List<IParameter> list = new ArrayList<IParameter>();
		list.add(new EmployeeMapper());
		list.add(new DepartmentsMapper());
		list.add(new LocationsMapper());
		list.add(new OutputPath("Output", ""));
		Loader loader = new Loader(list, args);

		loader.load();

		// get input data
		DataSet<Tuple2<String, Integer>> names = loader.data
				.get(0)
				.project(1, 2, 10)
				.types(String.class, String.class, Integer.class)
				.map(new MapFunction<Tuple3<String, String, Integer>, Tuple2<String, Integer>>() {
					public Tuple2<String, Integer> map(
							Tuple3<String, String, Integer> line)
							throws Exception {
						return new Tuple2<String, Integer>(line.f0 + " "
								+ line.f1, line.f2);
					}

				});

		DataSet dep_d = loader.data.get(1).project(0, 3)
				.types(Integer.class, Integer.class);

		DataSet loc_d = loader.data.get(2).project(0, 5)
				.types(Integer.class, String.class);

		final ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();
		DataSet<Tuple2<Integer, String>> myNull = env
				.fromElements(new Tuple2<Integer, String>(-1, "-1"));

		DataSet deploc = dep_d.join(loc_d).where(1).equalTo(0).projectFirst(0)
				.projectSecond(1).types(Integer.class, String.class);

		deploc.union(myNull);

		DataSet<Tuple2<String, String>> result = names.join(deploc).where(1)
				.equalTo(0).projectFirst(0).projectSecond(1)
				.types(String.class, String.class);

		// emit result
		result.writeAsCsv(loader.output.get(0).getPath(), "\n", " | ",
				WriteMode.OVERWRITE);

		// execute program
		env.execute(A2NameWithCountry.class.toString());
	}

}