package hadoop.stack.scoreDistribution;

import hadoop.mahout.XmlInputFormat;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CoffeeXML {

	public static void main(String[] args) {
		try {
			runJob(args);

		} catch (IOException ex) {
			Logger.getLogger(CoffeeXML.class.getName())
					.log(Level.SEVERE, null, ex);
		}

	}

	public static void runJob(String[] args)
			throws IOException {

		Configuration conf = new Configuration();

		conf.set("xmlinput.start", "<row");
		conf.set("xmlinput.end", "\" />");
		conf.set("io.serializations",
				"org.apache.hadoop.io.serializer."
						+ "JavaSerialization,org.apache."
						+ "hadoop.io.serializer."
						+ "WritableSerialization");

		Job job = Job.getInstance(conf,
				"Coffee Group and Desc Sort");

		job.setInputFormatClass(XmlInputFormat.class);

		job.setJarByClass(CoffeeXML.class);
		job.setMapperClass(CoffeeMapper.class);
		job.setReducerClass(CoffeeReducer.class);

		job.setSortComparatorClass(SortComparator.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		job.setOutputFormatClass(TextOutputFormat.class);

		Path outPath = new Path(args[1]);
		FileSystem dfs = FileSystem.get(outPath.toUri(),
				conf);
		if (dfs.exists(outPath)) {
			dfs.delete(outPath, true);
		}

		FileInputFormat.setInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, outPath);

		try {

			job.waitForCompletion(true);

		} catch (InterruptedException ex) {
			Logger.getLogger(CoffeeMapper.class.getName())
					.log(Level.SEVERE, null, ex);
		} catch (ClassNotFoundException ex) {
			Logger.getLogger(CoffeeMapper.class.getName())
					.log(Level.SEVERE, null, ex);
		}

	}

}