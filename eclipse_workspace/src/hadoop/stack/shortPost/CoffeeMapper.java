package hadoop.stack.shortPost;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

public class CoffeeMapper extends
		Mapper<LongWritable, Text, NullWritable, PostWritable> {

	SAXBuilder builder = new SAXBuilder();
	
	CoffeeMapper(){
		
	}

	@Override
	public void map(LongWritable key, Text val, Context context)

	throws IOException, InterruptedException {

		String xmlString = val.toString();

		Reader in = new StringReader(xmlString);
		try {
			Document doc = builder.build(in); // SAXBuilder builder
			Element root = doc.getRootElement();

			String body = root.getAttribute("Body").getValue();
			int min = context.getConfiguration().
					getInt("minLen",0);
			int max = context.getConfiguration().
					getInt("maxLen",Integer.MAX_VALUE);

			if (body.length() <= max && body.length() >= min) {
				
				PostWritable post = new PostWritable();
				post.id = new IntWritable((Integer.parseInt(root.
						getAttribute("Id").getValue())));
				post.score = new IntWritable(Integer.parseInt(root
						.getAttribute("Score").getValue()));
				post.body = new Text(body);

				context.write(NullWritable.get(), post);
			}

		} catch (JDOMException ex) {
			Logger.getLogger(CoffeeMapper.class.getName()).log(Level.SEVERE,
					null, ex);
		} catch (IOException ex) {
			Logger.getLogger(CoffeeMapper.class.getName()).log(Level.SEVERE,
					null, ex);
		}

	}

}