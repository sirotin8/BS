package hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SecondarySort {

	public static class KeyPair implements WritableComparable<KeyPair> {

		private Text first;
		private IntWritable second;

		@Override
		public void write(DataOutput out) throws IOException {

		}

		@Override
		public void readFields(DataInput in) throws IOException {

		}

		public Text getFirst() {
			return this.first;
		}

		public IntWritable getSecond() {
			return this.second;
		}

		public static int compare(Text first2, Text first3) {
			return first2.compareTo(first3);
		}

		public static int compare(IntWritable second2, IntWritable second3) {
			return second2.compareTo(second3);
		}

		@Override
		public int compareTo(KeyPair o) {
			int cmp = KeyPair.compare(this.getFirst(), o.getFirst());
			if (cmp != 0) {
				return cmp;
			}
			return -KeyPair.compare(this.getSecond(), o.getSecond());
		}

	}

	public static class FirstPartitioner extends
			Partitioner<KeyPair, NullWritable> {
		@Override
		public int getPartition(KeyPair key, NullWritable value,
				int numPartitions) {
			// multiply by 127 to perform some mixing
			return Math.abs(key.getSecond().get() * 127) % numPartitions;
		}
	}

	public static class KeyComparator extends WritableComparator {
		protected KeyComparator() {
			super(KeyPair.class, true);
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			KeyPair ip1 = (KeyPair) w1;
			KeyPair ip2 = (KeyPair) w2;
			return ip1.compareTo(ip2);
		}
	}

	public static class GroupComparator extends WritableComparator {
		protected GroupComparator() {
			super(KeyPair.class, true);
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			KeyPair ip1 = (KeyPair) w1;
			KeyPair ip2 = (KeyPair) w2;
			return KeyPair.compare(ip1.getFirst(), ip2.getFirst());
		}
	}

	public static class Selektor extends
			Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable einsKonstante = new IntWritable(1);
		private Text einWort = new Text();

		public void map(Object schluessel, Text wert, Context kontext)
				throws IOException, InterruptedException {

			String zeile = wert.toString();
			StringTokenizer zerleger = new StringTokenizer(zeile);
			while (zerleger.hasMoreTokens()) {
				einWort.set(zerleger.nextToken());
				kontext.write(einWort, einsKonstante);

			}
		}
	}

	public static class Reduktion extends
			Reducer<Text, IntWritable, IntWritable, Text> {
		IntWritable ergebnis = new IntWritable();

		public void reduce(Text word, Iterable<IntWritable> occurence,
				Context kontext) throws IOException, InterruptedException {
			int summe = 0;
			for (IntWritable tmp : occurence) {
				summe += tmp.get();
			}
			ergebnis.set(summe);
			kontext.write(ergebnis, word);
		}
	}

	public static void main(String[] args) throws Exception {

		System.out.println("blabla");

		Configuration conf = new Configuration();
		String[] otherArgs = args;
		if (otherArgs.length != 2) {
			System.err
					.println("Usage: WortZaehlerBeispiel <eingabe> <ausgabe>");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "WortZaehlerBeispiel");

		job.getLocalCacheFiles();

		job.setJarByClass(SecondarySort.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		// SequenceFileInputFormat
		job.setMapperClass(Selektor.class);
		job.setReducerClass(Reduktion.class);

		job.setPartitionerClass(FirstPartitioner.class);
		job.setSortComparatorClass(KeyComparator.class);
		job.setGroupingComparatorClass(GroupComparator.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
