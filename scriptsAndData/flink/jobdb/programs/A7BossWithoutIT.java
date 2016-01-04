package de.hshannover.vis.flink.jobdb;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.core.fs.FileSystem.WriteMode;

import de.hshannover.vis.flink.jobdb.mapper.EmployeeMapper;
import de.hshannover.vis.flink.jobdb.utils.IParameter;
import de.hshannover.vis.flink.jobdb.utils.Joiner;
import de.hshannover.vis.flink.jobdb.utils.Joiner.Mode;
import de.hshannover.vis.flink.jobdb.utils.Loader;
import de.hshannover.vis.flink.jobdb.utils.OutputPath;

public class A7BossWithoutIT {

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
		DataSet<Tuple2<String, Integer>> names = loader.data.get(0)
				.project(1,2,0).types(String.class,String.class
						,Integer.class)
				.map(new MapFunction<Tuple3<String,String, Integer>,
						Tuple2<String,Integer>>(){
					@Override
					public Tuple2<String,  Integer> map(
							Tuple3<String,String, Integer> empl) 
									throws Exception {
						return new Tuple2<String,  Integer>(
								empl.f0 + " " + empl.f1,empl.f2);
					}
					
				})
				;

		DataSet<Tuple2<Integer,String>> mng = loader.data.get(0)
				.project(9,6).types(Integer.class,String.class)
				.filter(new FilterFunction<Tuple2<Integer,String>>(){
					@Override
					public boolean filter(Tuple2<Integer,String> fil) throws Exception {
						if(fil.f0 == -1)
							return false;
						return true;
					}
				})
				.groupBy(0).max(1)
			;
		
		DataSet<Tuple2<String, Integer>> mngNames = names.join(mng).where(1)
				.equalTo(0).projectFirst(0,1).projectSecond(0)
				.types(String.class,Integer.class,Integer.class)
				.project(0,1).types(String.class,Integer.class)
				;
				
				DataSet<Tuple2<Integer,String>> itmng = mng
				.filter(new FilterFunction<Tuple2<Integer,String>>(){
					@Override
					public boolean filter(Tuple2<Integer,String> fil) throws Exception {
						if(fil.f1.contains("IT_PROG"))
							return true;
						return false;
					}
				})
//				.groupBy(0).max(1)
				;
		
		
		DataSet result = mngNames
				.project(0,1,1,0).types(String.class,Integer.class,
						Integer.class,String.class)
				.coGroup(itmng).where(1).equalTo(0)
				.with(new Joiner<Tuple4<String,Integer,Integer,String>,
						Tuple2<Integer,String>>
				(Mode.LEFTONLY,new Tuple2<Integer,String>(-1,"-1")))
				.project(0).types(String.class);
				;

		// emit result
		result.writeAsCsv(loader.output.get(0).getPath(), "\n", " | ",
				WriteMode.OVERWRITE);
//		itmng.writeAsCsv(loader.output.get(0).getPath()+"2", "\n", " | ",
//				WriteMode.OVERWRITE);
//		mngNames.writeAsCsv(loader.output.get(0).getPath()+"3", "\n", " | ",
//				WriteMode.OVERWRITE);

		// execute program
		env.execute(A7BossWithoutIT.class.toString());
	}
}