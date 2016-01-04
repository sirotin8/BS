package hadoop.stack.popular;

import hadoop.mahout.XmlInputFormat;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CoffeeXML {

	public static void main(String[] args) {
		try {

			String tmpFile = "tmpFile";
			String input1 = args[0];
			String input2 = args[1];
			String output = args[2];

			runSelect(input1, input2, tmpFile);

			if (!test)
				runAggregate(tmpFile, output);

		} catch (IOException ex) {
			Logger.getLogger(CoffeeXML.class.getName()).log(
					Level.SEVERE, null, ex);
		}

	}

	public static String selUserId;
	public static boolean test = false;
	public static boolean test2 = false;

	public static void runAggregate(String input, String output)
			throws IOException {

		if (input == null) {
			System.out
					.println("intermediate file from previos job not create!");
			System.exit(2);
		}

		Configuration conf = new Configuration();

		conf.set("xmlinput.start", "<row");
		conf.set("xmlinput.end", "\" />");
		conf.set("io.serializations",
				"org.apache.hadoop.io.serializer."
						+ "JavaSerialization,org.apache."
						+ "hadoop.io.serializer."
						+ "WritableSerialization");

		Job job = Job.getInstance(conf, "Join 2 - Aggegrate Post");

		job.setJarByClass(CoffeeXML.class);

		Path intermediate = new Path(input);
		Path outPath = new Path(output);
		FileSystem dfs = FileSystem.get(outPath.toUri(), conf);
		if (dfs.exists(outPath)) {
			dfs.delete(outPath, true);
		}

		MultipleInputs.addInputPath(job, intermediate,
				SequenceFileInputFormat.class, OwnerAsKeyMapper.class);
		FileOutputFormat.setOutputPath(job, outPath);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(PostWritable.class);

		job.setReducerClass(SumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(ScoreWritable.class);

		job.setOutputFormatClass(TextOutputFormat.class);

		try {

			job.waitForCompletion(true);

		} catch (InterruptedException ex) {
			Logger.getLogger(CoffeeXML.class.getName()).log(
					Level.SEVERE, null, ex);
		} catch (ClassNotFoundException ex) {
			Logger.getLogger(CoffeeXML.class.getName()).log(
					Level.SEVERE, null, ex);
		}

	}

	public static void runSelect(String input1, String input2,
			String tmpFile) throws IOException {

		Configuration conf = new Configuration();

		conf.set("xmlinput.start", "<row");
		conf.set("xmlinput.end", "\" />");
		conf.set("io.serializations",
				"org.apache.hadoop.io.serializer."
						+ "JavaSerialization,org.apache."
						+ "hadoop.io.serializer."
						+ "WritableSerialization");

		Job job = Job.getInstance(conf, "Join 1 - Posts Select");

		job.setJarByClass(CoffeeXML.class);

		Path postFile = new Path(input1);
		Path acceptFile = new Path(input2);
		Path outPath = new Path(tmpFile);
		FileSystem dfs = FileSystem.get(outPath.toUri(), conf);
		if (dfs.exists(outPath)) {
			dfs.delete(outPath, true);
		}

		MultipleInputs.addInputPath(job, acceptFile,
				XmlInputFormat.class, JoinAccepterMapper.class);
		MultipleInputs.addInputPath(job, postFile,
				XmlInputFormat.class, JoinPostMapper.class);
		FileOutputFormat.setOutputPath(job, outPath);

		job.setPartitionerClass(KeyPartitioner.class);
		job.setGroupingComparatorClass(TextPair.GroupComparator.class);
		job.setSortComparatorClass(TextPair.GroupComparator.class);

		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(PostWritable.class);

		if (test2)
			job.setNumReduceTasks(0);
		else {
			job.setReducerClass(JoinReducer.class);
			job.setOutputKeyClass(TextPair.class);
			job.setOutputValueClass(PostWritable.class);
		}

		if (test)
			job.setOutputFormatClass(TextOutputFormat.class);
		else
			job.setOutputFormatClass(SequenceFileOutputFormat.class);

		try {

			job.waitForCompletion(true);
		} catch (InterruptedException ex) {
			Logger.getLogger(CoffeeXML.class.getName()).log(
					Level.SEVERE, null, ex);
		} catch (ClassNotFoundException ex) {
			Logger.getLogger(CoffeeXML.class.getName()).log(
					Level.SEVERE, null, ex);
		}

	}

}