package hadoop.stack.demo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends
		Reducer<TextPair, PostWritable, Text, Text> {

	@Override
	protected void reduce(TextPair key,
			Iterable<PostWritable> values, Context context)
			throws IOException, InterruptedException {

		ArrayList<PostWritable> questions = new ArrayList<PostWritable>();
		ArrayList<PostWritable> answers = new ArrayList<PostWritable>();
		Iterator<PostWritable> it = values.iterator();
		while (it.hasNext()) {
			PostWritable post = new PostWritable(it.next());
			if (post.tag.toString().equals(JoinPostMapper.ACCE))
				questions.add(post);
			if (post.tag.toString().equals(JoinPostMapper.POST))
				answers.add(post);
		}

		// if(question.isEmpty())
		// question.add(new PostWritable());
		// if(answer.isEmpty())
		// answer.add(new PostWritable());

		if (questions.size() >= 1 && answers.size() == 1) {

			for (int i = 0; i < answers.size(); i++) {

				for (int j = 0; j < questions.size(); j++) {

					String qBody = questions.get(j).getBody();
					String aBody = answers.get(i).getBody();

					Text body = new Text(qBody + " " + aBody);

					context.write(key.first, body);
				}
			}
		}

	}

}
