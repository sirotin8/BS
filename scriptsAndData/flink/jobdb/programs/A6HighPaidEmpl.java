package de.hshannover.vis.flink.jobdb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import de.hshannover.vis.flink.jobdb.mapper.DepartmentsMapper;
import de.hshannover.vis.flink.jobdb.mapper.EmployeeMapper;
import de.hshannover.vis.flink.jobdb.mapper.LocationsMapper;
import de.hshannover.vis.flink.jobdb.utils.IParameter;
import de.hshannover.vis.flink.jobdb.utils.Loader;
import de.hshannover.vis.flink.jobdb.utils.OutputPath;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;

public class A6HighPaidEmpl {

	@SuppressWarnings({ "unchecked", "serial", "rawtypes" })
	public static void main(String[] args) throws Exception {

		List<IParameter> list = new ArrayList<IParameter>();
		list.add(new EmployeeMapper());
		list.add(new DepartmentsMapper());
		list.add(new LocationsMapper());
		list.add(new OutputPath("Output", ""));
		Loader loader = new Loader(list, args);

		loader.load();

		final ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();
		DataSet<Tuple2<Integer, String>> myNull = env
				.fromElements(new Tuple2<Integer, String>(-1, "-1"));

		// get input data
		DataSet<Tuple3<String, Integer, Integer>> names = loader.data
				.get(0)
				.project(1, 2, 7, 10)
				.types(String.class, String.class, Integer.class, Integer.class)
				.map(new MapFunction<Tuple4<String, String, Integer, Integer>, Tuple3<String, Integer, Integer>>() {
					public Tuple3<String, Integer, Integer> map(
							Tuple4<String, String, Integer, Integer> line)
							throws Exception {
						return new Tuple3<String, Integer, Integer>(line.f0
								+ " " + line.f1, line.f2, line.f3);
					}

				});

		DataSet dep_d = loader.data.get(1).project(0, 3)
				.types(Integer.class, Integer.class);

		DataSet loc_d = loader.data.get(2).project(0, 5)
				.types(Integer.class, String.class);

		DataSet deploc = dep_d.join(loc_d).where(1).equalTo(0).projectFirst(0)
				.projectSecond(1).types(Integer.class, String.class);

		deploc.union(myNull);

		DataSet<Tuple3<String, Integer, String>> emplCnt = names.join(deploc)
				.where(2).equalTo(0).projectFirst(0, 1).projectSecond(1)
				.types(String.class, Integer.class, String.class);

		DataSet avgSal = emplCnt
				.project(1, 2)
				.types(Integer.class, String.class)
				.groupBy(1)
				.reduceGroup(
						new GroupReduceFunction<Tuple2<Integer, String>, Tuple2<Integer, String>>() {

							@Override
							public void reduce(
									Iterable<Tuple2<Integer, String>> in,
									Collector<Tuple2<Integer, String>> out)
									throws Exception {
								int i = 0;
								int sum = 0;
								Iterator<Tuple2<Integer, String>> iterator = in
										.iterator();
								if (iterator.hasNext()) {
									Tuple2<Integer, String> tmp = iterator
											.next();
									String cnt = tmp.f1;
									sum += tmp.f0;
									i++;
									while (iterator.hasNext()) {
										tmp = iterator.next();
										sum += tmp.f0;
										i++;
									}
									sum = (int) ((sum / i) + 0.5);
									out.collect(new Tuple2<Integer, String>(
											new Integer(sum), cnt));

								}
							}
						});

		DataSet<Tuple4<String, Integer, Integer, String>> emplAvgSal = emplCnt
				.join(avgSal)
				.where(2)
				.equalTo(1)
				.projectFirst(0, 1)
				.projectSecond(0, 1)
				.types(String.class, Integer.class, Integer.class, String.class);

		DataSet result = emplAvgSal
				.flatMap(new FlatMapFunction<Tuple4<String, Integer, Integer, String>, Tuple4<String, Integer, Integer, String>>() {
					@Override
					public void flatMap(
							Tuple4<String, Integer, Integer, String> in,
							Collector<Tuple4<String, Integer, Integer, String>> out)
							throws Exception {
						if (in.f1 > in.f2)
							out.collect(in);
					}

				})

		;

		// emit result
		result.writeAsCsv(loader.output.get(0).getPath(), "\n", " | ",
				WriteMode.OVERWRITE);
		// emplAvgSal.writeAsCsv(loader.output.get(0).getPath()+"empl", "\n",
		// " | ");

		// execute program
		env.execute(A6HighPaidEmpl.class.toString());
	}
}