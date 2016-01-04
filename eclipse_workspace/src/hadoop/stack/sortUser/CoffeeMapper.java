package hadoop.stack.sortUser;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

public class CoffeeMapper extends
		Mapper<LongWritable, Text, Text,NullWritable > {

	SAXBuilder builder = new SAXBuilder();
	
	@Override
	public void map(LongWritable key, Text val, Context context)

	throws IOException, InterruptedException {

		String xmlString = val.toString();

		Reader in = new StringReader(xmlString);
		try {
			Document doc = builder.build(in); // SAXBuilder builder
			Element root = doc.getRootElement();
			String name = root.getAttribute("DisplayName").getValue();
			context.write(new Text(name),NullWritable.get() );
			
		} catch (JDOMException ex) {
			Logger.getLogger(CoffeeMapper.class.getName()).log(Level.SEVERE,
					null, ex);
		} catch (IOException ex) {
			Logger.getLogger(CoffeeMapper.class.getName()).log(Level.SEVERE,
					null, ex);
		}

	}

}