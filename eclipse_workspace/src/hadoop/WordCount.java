package hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class WordCount {

	public static class DescComparator extends WritableComparator {

		protected DescComparator() {
	        super(IntWritable.class);
	    }

	    @Override
	    public int compare(byte[] b1, int s1, int l1,
	            byte[] b2, int s2, int l2) {

	        Integer v1 = ByteBuffer.wrap(b1, s1, l1).getInt();
	        Integer v2 = ByteBuffer.wrap(b2, s2, l2).getInt();

	        return -v1.compareTo(v2);
	    }
	}

	public static class Sortierer extends
			Mapper<Object, Text, IntWritable, Text> {

		public void map(Object obj, Text line, Context kontext)
				throws IOException, InterruptedException {
			String[] splits = line.toString().split("\t");
			Text wort = new Text(splits[0]);			
			IntWritable anzahl = new IntWritable(Integer.parseInt(splits[1]));
			kontext.write(anzahl, wort);
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
			Reducer<Text, IntWritable, Text, IntWritable> {
		IntWritable ergebnis = new IntWritable();

		public void reduce(Text word, Iterable<IntWritable> occurence,
				Context kontext) throws IOException, InterruptedException {
			int summe = 0;
			for (IntWritable tmp : occurence) {
				summe += tmp.get();
			}
			ergebnis.set(summe);
			kontext.write(word, ergebnis);
		}
	}

	public static void main(String[] args) throws Exception {

		final String TXT_SORT = "-sortonly";

		Configuration conf = new Configuration();

		String[] otherArgs = args;
		if (otherArgs.length != 2 && otherArgs.length != 3) {
			System.err
					.println("Usage: WortZaehlerBeispiel <eingabe> <ausgabe> ["
							+ TXT_SORT + "]");
			System.exit(2);
		}

		String tmpFname = args[1] + "_tmp";

		Job job = Job.getInstance(conf, "WortZaehlerBeispiel");

		if (!(args.length == 3 && args[2].equals(TXT_SORT))) {

			job.getLocalCacheFiles();

			job.setJarByClass(WordCount.class);

			job.setInputFormatClass(SequenceFileInputFormat.class);
			// job.setOutputFormatClass(SequenceFileOutputFormat.class);

			job.setMapperClass(Selektor.class);
			job.setReducerClass(Reduktion.class);

			// job.setMapOutputKeyClass(Text.class);
			// job.setMapOutputValueClass(IntWritable.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

			FileInputFormat.setInputPaths(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(tmpFname));

			if (!job.waitForCompletion(true)) {
				System.out.println("Error ");
				System.exit(1);
			}

		}

		job = Job.getInstance(conf, "WortZaehlerBeispiel Sortieren");
		job.getLocalCacheFiles();
		job.setJarByClass(WordCount.class);

		job.setMapperClass(Sortierer.class);
		job.setSortComparatorClass(DescComparator.class);

		// job.setMapOutputKeyClass(Text.class);
		// job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path(tmpFname));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
