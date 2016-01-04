package hadoop.stack.popular;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinPostMapper extends
		Mapper<LongWritable, Text, TextPair, PostWritable> {

	public static final String POST = "1__ANSWER";
	public static final String ACCE = "0QUESTION";
	String tag = POST;

	@Override
	public void map(LongWritable key, Text val, Context context)

	throws IOException, InterruptedException {

		PostWritable post = new PostWritable(val.toString());

		if (post.isOk && post.typeId.toString().equals("2")) {
			post.tag = new Text(tag);

			TextPair pair = new TextPair();
			pair.first = post.id;
			pair.second = new Text(tag);

			context.write(pair, post);
		}
	}
}
