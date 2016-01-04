package hadoop.stack.popular;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OwnerAsKeyMapper extends
		Mapper<TextPair, PostWritable, Text,PostWritable> {

	@Override
	public void map(TextPair key, PostWritable val, Context context)

	throws IOException, InterruptedException {
		
		context.write(new Text(val.ownerId),val);

	}

}