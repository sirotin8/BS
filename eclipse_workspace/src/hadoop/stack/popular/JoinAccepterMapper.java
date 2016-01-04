package hadoop.stack.popular;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinAccepterMapper extends
		Mapper<LongWritable, Text, TextPair, PostWritable> {

	String tag = JoinPostMapper.ACCE;

	@Override
	public void map(LongWritable key, Text val, Context context)

	throws IOException, InterruptedException {

		String xmlString = val.toString();
		PostWritable post = new PostWritable(xmlString);

		if (post.isOk) {

			post.tag = new Text(tag);

			TextPair pair = new TextPair();
			pair.first = post.accept;
			pair.second = new Text(tag);

			context.write(pair, post);

		}
	}
}