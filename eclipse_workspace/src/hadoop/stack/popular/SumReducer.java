package hadoop.stack.popular;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SumReducer extends
		Reducer<Text,PostWritable,Text,ScoreWritable> {

	@Override
	protected void reduce(Text key, Iterable<PostWritable> values,
			Context context) throws IOException, InterruptedException {

		Iterator<PostWritable> i = values.iterator();

		int accepted  = 0;
		int scored = 0;
		int unscored = 0;
		while(i.hasNext()){
			accepted++;
			PostWritable val = i.next();
			if(val.score.get() == 0) unscored ++;
			else scored++;
		}
		ScoreWritable score = new ScoreWritable();
		
		int percent = scored * 100 / accepted;
		
		score.accepted = new IntWritable(accepted);
		score.scored = new IntWritable(scored);
		score.unscored = new IntWritable(unscored);
		score.percentUnscored = new IntWritable(percent);

		context.write(new Text(key),score);

	}

}
