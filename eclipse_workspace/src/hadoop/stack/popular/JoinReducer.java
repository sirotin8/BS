package hadoop.stack.popular;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends
		Reducer<TextPair, PostWritable, TextPair, PostWritable> {

	@Override
	protected void reduce(TextPair key, Iterable<PostWritable> values,
			Context context) throws IOException, InterruptedException {

		ArrayList<PostWritable> questions = new ArrayList<PostWritable>();
		ArrayList<PostWritable> answers = new ArrayList<PostWritable>();
		Iterator<PostWritable> i = values.iterator();
		while (i.hasNext()) {
			PostWritable post = new PostWritable(i.next());
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

			for (int j = 0; j < questions.size(); j++) {
				
				if (!answers.get(0).ownerId.toString().equals(
						questions.get(j).ownerId.toString())) {

					TextPair pair = new TextPair();
					pair.first = new Text(answers.get(0).ownerId);
					pair.second = new Text(String.valueOf(j));
					context.write(pair, answers.get(0));

				} else if (CoffeeXML.test) {

					PostWritable post = new PostWritable();
					post.body = new Text(
							"answer and question from same owner ");
					context.write(new TextPair(), post);

					// Iterator<PostWritable> j = values.iterator();
					// while (i.hasNext()) {
					// context.write(new TextPair(), new
					// PostWritable(i.next()));
					// }
				}
			}

		} else if (CoffeeXML.test) {
			PostWritable post = new PostWritable();
			post.body = new Text("q-size " + questions.size()
					+ " | a-size " + answers.size());
			context.write(new TextPair(), post);

			i = values.iterator();
			while (i.hasNext()) {
				context.write(new TextPair(),
						new PostWritable(i.next()));
			}

		}

	}

}
