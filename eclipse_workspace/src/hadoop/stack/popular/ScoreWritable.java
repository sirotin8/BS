package hadoop.stack.popular;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class ScoreWritable implements WritableComparable<ScoreWritable>{

	public IntWritable accepted, scored, unscored,percentUnscored;
	public Text body = new Text();
	
	public ScoreWritable(){
		accepted = new IntWritable();
		scored = new IntWritable();
		percentUnscored = new IntWritable();
		unscored = new IntWritable();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		accepted.write(out);
		scored.write(out);
		unscored.write(out);
		percentUnscored.write(out);
		body.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		accepted.readFields(in);
		scored.readFields(in);
		unscored.readFields(in);
		percentUnscored.readFields(in);
		body.readFields(in);
	}

	@Override
	public int compareTo(ScoreWritable other) {
		int result = accepted.compareTo(other.accepted);
		if(result == 0) result = scored.compareTo(other.scored);
		if(result == 0) result = unscored.compareTo(other.unscored);
		if(result == 0) result = percentUnscored.
				compareTo(other.percentUnscored);
		return result;
	}
	
	@Override
	public String toString(){
		return  " :=: " + accepted + " | " + scored + " | " +
				unscored + " | " + percentUnscored + " | " +
				body.toString();
	}

}
