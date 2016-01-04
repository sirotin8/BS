package de.hshannover.vis.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.util.Collector;

@SuppressWarnings("serial")
public class WordCount {

	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		// get input data
		DataSet<String> text = getTextDataSet(env);

		DataSet<Tuple2<String, Integer>> counts =
		// split up the lines in pairs (2-tuples) containing: (word,1)
		text.flatMap(new Tokenizer())
		// group by the tuple field "0" and sum up tuple field "1"
				.groupBy(0).aggregate(Aggregations.SUM, 1);

		// emit result
		if (fileOutput) {
			counts.writeAsCsv(outputPath, "\n", " ");
		} else {
			counts.print();
		}

		// execute program
		env.execute("WordCount Example");
	}

	public static final class Tokenizer implements
			FlatMapFunction<String, Tuple2<String, Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");
			int number = 1;

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					if (token.equals("data"))
						number = 99999;
					else
						number = 1;
					out.collect(new Tuple2<String, Integer>(token, number));
				}
			}
		}
	}

	private static boolean fileOutput = false;
	private static String textPath;
	private static String outputPath;

	private static boolean parseParameters(String[] args) {

		if (args.length > 0) {
			// parse input arguments
			fileOutput = true;
			if (args.length == 2) {
				textPath = args[0];
				outputPath = args[1];
			} else {
				System.err
						.println("Usage: WordCount <text path> <result path>");
				return false;
			}
		} else {
			System.out
					.println("Executing WordCount example with built-in default data.");
			System.out
					.println(" Provide parameters to read input data from a file.");
			System.out.println(" Usage: WordCount <text path> <result path>");
		}
		return true;
	}

	private static DataSet<String> getTextDataSet(ExecutionEnvironment env) {
		if (fileOutput) {
			// read the text file from given input path
			return env.readTextFile(textPath);
		} else {
			// get default test text data
			return null;
		}
	}
}