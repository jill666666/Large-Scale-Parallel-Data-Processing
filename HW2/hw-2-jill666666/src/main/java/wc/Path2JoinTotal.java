package wc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class Path2JoinTotal extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(Path2JoinTotal.class);

	public static class Path2Mapper extends Mapper<Object, Text, Text, Text> {
		private final Text keyText = new Text();
		private final Text valueText = new Text();
		final String LINE_DELIMITER = "\n";
		final String COMMA_DELIMITER = ",";

		@Override
		public void map(final Object key, final Text value, final Context context)
				throws IOException, InterruptedException {

			// given the input string, split by whitespace "\n" and store each of the line
			// to the tokens
			String[] tokens = value.toString().split(LINE_DELIMITER);

			// iterate through the tokens to get the 'from' and 'to' nodes
			for (int i = 0; i < tokens.length; i++) {
				String[] nodes = tokens[i].split(COMMA_DELIMITER);
				String from = nodes[0];
				String to = nodes[1];

				// write context where key is source node and value is destination node
				// concatenated with "F" ("From")
				// ex. for edge (1, 2) --> write ("1", "2F")
				keyText.set(from.toString());
				valueText.set("F" + to.toString());
				context.write(keyText, valueText);

				// write context where key is destination node and value is source nnode
				// concatenated with "T" ("To")
				// ex. for edge (1, 2) --> write ("2", "1T")
				keyText.set(to.toString());
				valueText.set("T" + from.toString());
				context.write(keyText, valueText);
			}
		}
	}

	public static class Path2Reducer extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(final Text key, final Iterable<Text> values, final Context context)
				throws IOException, InterruptedException {

			int fromCount = 0;
			int toCount = 0;

			for (Text v : values) {
				String flag = v.toString().substring(0, 1);
				if (flag.equals("F")) {
					// fromArray.add(node);
					fromCount += 1;
				} else {
					// toArray.add(node);
					toCount += 1;
				}
			}

			// calculate the number of pairs and update the global counter
			// only used to count the Path2 cardinality on entire dataset (does not write results to output file)
			if (fromCount != 0 && toCount != 0) {
				context.getCounter("counter", "PATH2_CARDINALITY").increment(new Long(fromCount * toCount));
			}
		}
	}

	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "Path2 Join");
		job.setJarByClass(Path2JoinTotal.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", ",");
		// Delete output directory, only to ease local development; will not work on
		// AWS. ===========
		// final FileSystem fileSystem = FileSystem.get(conf);
		// if (fileSystem.exists(new Path(args[1]))) {
		// fileSystem.delete(new Path(args[1]), true);
		// }
		// ================
		job.setMapperClass(Path2Mapper.class);
		// job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(Path2Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		if (job.waitForCompletion(true)) {
			long path2Cardinality = job.getCounters().findCounter("counter", "PATH2_CARDINALITY").getValue();
			logger.info("path2 cardinality: " + path2Cardinality);
			return 0;
		}
		return 1;
	}

	public static void main(final String[] args) {
		if (args.length != 2) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}

		try {
			ToolRunner.run(new Path2JoinTotal(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}