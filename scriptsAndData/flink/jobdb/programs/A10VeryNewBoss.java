package de.hshannover.vis.flink.jobdb;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.core.fs.FileSystem.WriteMode;

import de.hshannover.vis.flink.jobdb.mapper.EmployeeMapper;
import de.hshannover.vis.flink.jobdb.utils.IParameter;
import de.hshannover.vis.flink.jobdb.utils.Loader;
import de.hshannover.vis.flink.jobdb.utils.OutputPath;

public class A10VeryNewBoss {

	@SuppressWarnings({ "unchecked", "rawtypes", "serial" })
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
		DataSet<Tuple4<Integer,String, Long,Integer>> names = loader.data.get(0)
				.project(0,1,2,5,9).types(Integer.class,String.class,String.class
						,String.class,Integer.class)
				.map(new MapFunction<Tuple5<Integer,String,String, String,Integer>,
						Tuple4<Integer,String, Long,Integer>>(){
					@Override
					public Tuple4<Integer,String, Long,Integer> map(
							Tuple5<Integer,String,String, String,Integer> empl) 
									throws Exception {
						Date date = new SimpleDateFormat("dd.MM.yy").
								parse(empl.f3);
						
						return new Tuple4<Integer,String, Long,Integer>(
								empl.f0,empl.f1 + " " + empl.f2,
								new Long(date.getTime()),
								empl.f4);
					}
				})
				;
		
		DataSet<Tuple2<Integer,Long>> empl = names.project(3,2).types(Integer.class,Long.class)
				.groupBy(0).max(1);
		
		DataSet result = names.join(empl).where(0).equalTo(0)
				.projectFirst(1,2).projectSecond(1)
				.types(String.class,Long.class,Long.class)
				.filter(new FilterFunction<Tuple3<String,Long,Long>>(){
					@Override
					public boolean filter(Tuple3<String, Long, Long> ch)
							throws Exception {
						if(ch.f1>ch.f2)
							return true;
						return false;
					}
				})
				;
		
		// emit result
		result.writeAsCsv(loader.output.get(0).getPath(), "\n", " | ",
				WriteMode.OVERWRITE);
//		itmng.writeAsCsv(loader.output.get(0).getPath()+"2", "\n", " | ",
//				WriteMode.OVERWRITE);
//		mngNames.writeAsCsv(loader.output.get(0).getPath()+"3", "\n", " | ",
//				WriteMode.OVERWRITE);

		// execute program
		env.execute(A10VeryNewBoss.class.toString());
	}
}