package hadoop.stack.scoreDistribution;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CoffeeMapper extends
		Mapper<LongWritable, Text, IntWritable, IntWritable> {

	@Override
	public void map(LongWritable key, Text val, Context context)
			throws IOException, InterruptedException {

		CommentWritable comment = new CommentWritable(val.toString());
		context.write(new IntWritable(comment.score.get()),
				new IntWritable(1));
	}

}