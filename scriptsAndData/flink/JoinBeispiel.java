package de.hshannover.vis.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.*;

@SuppressWarnings({ "rawtypes", "serial" })
public class JoinBeispiel {

	// Types are kept here, because of simplification and reduce complexity
	public static class Multiplier
			implements
			JoinFunction<Tuple4<Integer, Integer, String, Integer>, Tuple3<Integer, String, Double>, Tuple4<Integer, String, String, Double>> {

		@Override
		public Tuple4<Integer, String, String, Double> join(
				Tuple4<Integer, Integer, String, Integer> sale,
				Tuple3<Integer, String, Double> product) {
			return new Tuple4<Integer, String, String, Double>(sale.f0,
					product.f1, sale.f2, (Integer) sale.f3
							* (Double) product.f2);
		}
	}

	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		DataSet result;

		// get input data
		DataSet<String> strings = env.readTextFile(salesPath);
		DataSet<Tuple4<Integer, Integer, String, Integer>> sales = strings
				.map(new MapFunction<String, Tuple4<Integer, Integer, String, Integer>>() {
					public Tuple4<Integer, Integer, String, Integer> map(
							String line) throws Exception {
						String[] arr = line.split(" ");
						return new Tuple4<Integer, Integer, String, Integer>(
								Integer.parseInt(arr[0]), Integer
										.parseInt(arr[1]), arr[2], Integer
										.parseInt(arr[3]));
					}
				});

		strings = env.readTextFile(productsPath);
		DataSet<Tuple3<Integer, String, Double>> products = strings
				.map(new MapFunction<String, Tuple3<Integer, String, Double>>() {
					public Tuple3<Integer, String, Double> map(String line)
							throws Exception {
						String[] arr = line.split(" ");
						return new Tuple3<Integer, String, Double>(Integer
								.parseInt(arr[0]), arr[1], Double
								.parseDouble(arr[2]));
					}

				});

		// die Group-By und Filter-Funktionen koennen auskommentiert und das
		// Programm neu kompiliert werden
		result = sales
				.join(products)
				// funktioniert nur mit Tuple's

				// where und equal haengen zusammen ...
				.where(1)
				.equalTo(0)

				// zusaetztlich zum join kann noch eine Projektion hinzugefuegt
				// werden mittels projectFirst und projectSecond oder mit with
				.with(new Multiplier())

				// project und types haengen zusammen
				.project(1, 2, 3)
				.types(String.class, String.class, Double.class)

				// Filter-Funktion
				.filter(new FilterFunction<Tuple3<String, String, Double>>() {
					@Override
					public boolean filter(Tuple3<String, String, Double> ps) {
						if (ps.f1.equals("2014/03/09"))
							return false;
						return true;
					}
				})

				// groupBy und reduce haengen zusammen
				// alternative zum reduce kann ein aggregate verwendet werden
				// zum aggregate koennen weitere Aggregate mit and hinzugefuegt
				// werden
				.groupBy(0).aggregate(Aggregations.SUM, 2)
				.and(Aggregations.MAX, 1)

		;

		// emit result
		if (fileOutput) {
			result.writeAsCsv(outputPath, "\n", " ");
		} else {
			result.print();
		}

		// execute program
		env.execute("WordCount Example");
	}

	private static boolean fileOutput = false;
	private static String productsPath;
	private static String salesPath;
	private static String outputPath;

	private static boolean parseParameters(String[] args) {

		// parse input arguments
		fileOutput = true;
		if (args.length == 3) {
			productsPath = args[0];
			salesPath = args[1];
			outputPath = args[2];
		} else {
			System.err
					.println("Aufruf: JoinBeispiel <Produkte-Pfad> <Verkauf-Pfad>"
							+ " <Ausgabe-Pfad>");
			return false;
		}
		return true;
	}
}