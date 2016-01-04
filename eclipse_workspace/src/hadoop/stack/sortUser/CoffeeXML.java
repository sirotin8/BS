package hadoop.stack.sortUser;

import hadoop.mahout.XmlInputFormat;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CoffeeXML {

	public static void main(String[] args) {
		try {
			runJob(args[0], args[1]);

		} catch (IOException ex) {
			Logger.getLogger(CoffeeXML.class.getName()).log(Level.SEVERE,
					null, ex);
		}

	}

	public static void runJob(String input, String output) throws IOException {

		Configuration conf = new Configuration();

		conf.set("xmlinput.start", "<row");
		conf.set("xmlinput.end", "\" />");
		conf.set("io.serializations",
				"org.apache.hadoop.io.serializer."
				+ "JavaSerialization,org.apache."
				+ "hadoop.io.serializer."
				+ "WritableSerialization");

		Job job = Job.getInstance(conf, "CoffeeParsing");

		job.setInputFormatClass(XmlInputFormat.class);
		
		job.setJarByClass(CoffeeXML.class);
		job.setMapperClass(CoffeeMapper.class);
//		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		Path outPath = new Path(output);
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, outPath);
		FileSystem dfs = FileSystem.get(outPath.toUri(), conf);
		if (dfs.exists(outPath)) {
			dfs.delete(outPath, true);
		}

		try {

			job.waitForCompletion(true);

		} catch (InterruptedException ex) {
			Logger.getLogger(CoffeeMapper.class.getName()).log(Level.SEVERE,
					null, ex);
		} catch (ClassNotFoundException ex) {
			Logger.getLogger(CoffeeMapper.class.getName()).log(Level.SEVERE,
					null, ex);
		}

	}

}