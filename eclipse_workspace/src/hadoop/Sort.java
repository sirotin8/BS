package hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Sort {

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

		public void reduce(Text schluessel, Iterable<IntWritable> werte,
				Context kontext) throws IOException, InterruptedException {
			int summe = 0;
			for (IntWritable tmp : werte) {
				summe += tmp.get();
			}
			ergebnis.set(summe);
			kontext.write(schluessel, ergebnis);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = args;
		if (otherArgs.length != 2) {
			System.err
					.println("Usage: WortZaehlerBeispiel <eingabe> <ausgabe>");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "WortZaehlerBeispiel");

		job.getLocalCacheFiles();

		job.setJarByClass(Sort.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Selektor.class);
		job.setCombinerClass(Reduktion.class);
		job.setReducerClass(Reduktion.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
