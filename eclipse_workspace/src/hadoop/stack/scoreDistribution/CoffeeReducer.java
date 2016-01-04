package hadoop.stack.scoreDistribution;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class CoffeeReducer extends
		Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

	@Override
	protected void reduce(IntWritable key,
			Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		Iterator<IntWritable> i = values.iterator();
		int count = 0;
		while (i.hasNext()) {
			count += i.next().get();
		}
		context.write(key, new IntWritable(count));

	}

}