package de.hshannover.vis.flink.jobdb.utils;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class Loader {

	public final static String EMPL = "Employee";
	public final static String CNTY = "Countries";
	public final static String LOCA = "Locations";
	public final static String DEPA = "Departments";
	public final static String JOBH = "Job-History";
	public final static String JOBS = "Jobs";
	public final static String REGI = "Regions";
	public final static String OUT1 = "Output";
	public final static String OUT2 = "Output 2";
	public final static String OUT3 = "Output 3";

	private List<IParameter> toLoad;
	public List<DataSet> data;
	public List<OutputPath> output;

	private String[] args;

	public Loader(List<IParameter> toLoad, String[] args) {
		this.toLoad = toLoad;
		this.args = args;

		check();
	}

	private void check() {

		if (args.length != toLoad.size()) {
			System.err.println("Usage: [program-name]");
			for (IParameter tmp : toLoad) {
				System.err.print(" <" + tmp.getParamName() + "-Path>");
			}

			System.err.println("\nYour paramaters: ");
			for (String name : args)
				System.err.print(name + "; ");

			throw new IllegalArgumentException();
		}

	}

	public void load() {

		final ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		this.data = new ArrayList<DataSet>();
		this.output = new ArrayList<OutputPath>();

		for (int i = 0; i < args.length; i++) {

			IParameter param = toLoad.get(i);
			if (param instanceof MapFunction) {
				MapFunction mapper = (MapFunction) param;
				String path = args[i];

				DataSet<String> strings = env.readTextFile(path);
				DataSet dataset = strings.map(mapper);
				data.add(dataset);
			}else if(param instanceof OutputPath){
				OutputPath path = (OutputPath)param;
				path.setPath(args[i]);
				output.add(path);
			}
		}

	}

}
