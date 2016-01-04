package hadoop.stack.shortPost;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class PostWritable implements WritableComparable<PostWritable>{
	
	public IntWritable id,score;
	public Text body;

	@Override
	public void write(DataOutput out) throws IOException {
		id.write(out);
		score.write(out);
		body.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		id.readFields(in);
		score.readFields(in);
		body.readFields(in);
	}

	@Override
	public int compareTo(PostWritable other) {
		int result = id.compareTo(other.id);
		if(result == 0) result = score.compareTo(other.score);
		if(result == 0) result = body.compareTo(other.body);
		return result;
	}
	
	@Override
	public String toString(){
		return id + ", " + score + ", " +
				body + "\n\n ------------- \n\n";
	}

}
