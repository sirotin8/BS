package de.hshannover.vis.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.util.Collector;

public class LineSplitter implements
		FlatMapFunction<String, Tuple2<String, Integer>> {

	private static final long serialVersionUID = 1L;

	@Override
	public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
		// normalize and split the line into words
		String[] tokens = value.toLowerCase().split(" ");

		int number = 1;
		// emit the pairs
		for (String token : tokens) {
			if (token.length() > 0) {
				if (token.equals("data"))
					number = 9999;
				else
					number = 1;
				out.collect(new Tuple2<String, Integer>(token, number));
			}
		}
	}

}
