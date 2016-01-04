package hadoop.stack.scoreDistribution;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.jdom.Attribute;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

public class CommentWritable implements
		WritableComparable<CommentWritable> {

	public static SAXBuilder builder = new SAXBuilder();

	public Text id = new Text();
	public Text postId = new Text();
	public Text userId = new Text();
	public Text creationDate = new Text();
	public IntWritable score = new IntWritable();
	public Text text = new Text();

	public Text tag = new Text();
	public boolean isOk = true;

	private Element root;

	public CommentWritable() {
	}

	public CommentWritable(String xml) {

		this.isOk = false;

		Reader in = new StringReader(xml);
		try {
			Document doc = builder.build(in);
			root = doc.getRootElement();

			this.id = get("Id");
			this.postId = get("PostId");
			this.userId = get("UserId");
			this.creationDate = get("CreationDate");

			this.score = getInt("Score");

			this.text = get("Text");

			this.isOk = true;

		} catch (JDOMException ex) {
			Logger.getLogger(CommentWritable.class.getName()).log(
					Level.SEVERE, null, ex);
			isOk = false;
		} catch (IOException e) {
			e.printStackTrace();
			isOk = false;
		} catch (NullPointerException e) {
			e.printStackTrace();
			isOk = false;
		}

	}

	public CommentWritable(CommentWritable copy) {
		this.id = new Text(copy.id);
		this.postId = new Text(copy.postId);
		this.userId = new Text(copy.userId);
		this.creationDate = new Text(copy.creationDate);
		this.score = new IntWritable(copy.score.get());
		this.text = new Text(copy.text);
		this.tag = new Text(copy.tag);
		this.isOk = copy.isOk;
	}

	private Text get(String attName) {
		Text result = new Text();
		Attribute att = root.getAttribute(attName);
		if (att != null) {
			result = new Text(att.getValue());
		}
		return result;
	}

	private IntWritable getInt(String attName) {
		IntWritable result = new IntWritable();
		Attribute att = root.getAttribute(attName);
		if (att != null) {
			result = new IntWritable(Integer.parseInt(att.getValue()));
		}
		return result;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		id.write(out);
		postId.write(out);
		userId.write(out);
		creationDate.write(out);
		score.write(out);
		text.write(out);
		tag.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		id.readFields(in);
		postId.readFields(in);
		userId.readFields(in);
		creationDate.readFields(in);
		score.readFields(in);
		text.readFields(in);
		tag.readFields(in);
	}

	@Override
	public int compareTo(CommentWritable other) {
		int result = postId.compareTo(other.postId);
		if (result == 0)
			result = id.compareTo(other.id);

		return result;
	}

	@Override
	public String toString() {
		int len = text.toString().length();
		if (len > 100)
			len = 100;
		return id + " | " + postId + " | " + userId + " | " + 
				creationDate + " | " + score + " | " + 
				" | " + tag + " | "
				+ text.toString().substring(0, len) + "\n\n"
				+ " ------------- \n\n";
	}

}
