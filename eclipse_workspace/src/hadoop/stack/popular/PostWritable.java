package hadoop.stack.popular;

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

public class PostWritable implements WritableComparable<PostWritable> {

	public static SAXBuilder builder = new SAXBuilder();

	public Text id = new Text();
	public Text typeId = new Text();
	public Text ownerId = new Text();
	public Text accept = new Text();
	public IntWritable score = new IntWritable();
	public Text body = new Text();
	public Text tag = new Text();
	public boolean isOk = true;

	public PostWritable() {
	}

	public PostWritable(String xml) {

		this.isOk = false;

		Reader in = new StringReader(xml);
		try {
			Document doc = builder.build(in);
			Element root = doc.getRootElement();

			this.id = new Text(root.getAttributeValue("Id"));
			this.typeId = new Text(root.getAttribute("PostTypeId").getValue());
			Attribute att = root.getAttribute("OwnerUserId");
			if (att != null) {
				this.ownerId = new Text(att.getValue());
			}
			Attribute att2 = root.getAttribute("AcceptedAnswerId");			
			if (att2 != null) {
				this.accept = new Text(att2.getValue());
			}			
			String strScore = root.getAttribute("Score").getValue();
			this.score = new IntWritable(Integer.parseInt(strScore));
			this.body = new Text(root.getAttribute("Body").getValue());

			this.isOk = true;

		} catch (JDOMException ex) {
			Logger.getLogger(JoinPostMapper.class.getName()).log(Level.SEVERE,
					null, ex);
			isOk = false;
		} catch (IOException e) {
			e.printStackTrace();
			isOk = false;
		} catch (NullPointerException e) {
			e.printStackTrace();
			isOk = false;
		}

	}

	public PostWritable(PostWritable copy) {
		this.id = new Text(copy.id);
		this.typeId = new Text(copy.typeId);
		this.ownerId = new Text(copy.ownerId);
		this.score = new IntWritable(copy.score.get());
		this.accept = new Text(copy.accept);
		this.body = new Text(copy.body);
		this.tag = new Text(copy.tag);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		id.write(out);
		typeId.write(out);
		ownerId.write(out);
		score.write(out);
		accept.write(out);
		body.write(out);
		tag.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		id.readFields(in);
		typeId.readFields(in);
		ownerId.readFields(in);
		score.readFields(in);
		accept.readFields(in);
		body.readFields(in);
		tag.readFields(in);
	}

	@Override
	public int compareTo(PostWritable other) {
		int result = ownerId.compareTo(other.ownerId);
		if (result == 0)
			result = score.compareTo(other.score);
		// if(result == 0) result = body.compareTo(other.body);
		return result;
	}

	@Override
	public String toString() {
		int len = body.toString().length();
		if (len > 100)
			len = 100;
		return id + " | " + typeId + " | " + ownerId + " | " + score + " | "
				+ accept + " | " + tag + " | "
				+ body.toString().substring(0, len) + "\n\n"
				+ " ------------- \n\n";
	}

}
