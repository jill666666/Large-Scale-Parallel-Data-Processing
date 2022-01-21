package pr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class PageRank extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(PageRank.class);

	// takes in the input text file and calculate the total number of vertices
	// and the dangling page PageRank mass sum.
	public static class DanglingPageMapper extends Mapper<Object, Text, IntWritable, Vertex> {
		final String LINE_DELIMITER = "\n";
		final String COMMA_DELIMITER = ",";

		@Override
		public void map(final Object key, final Text value, final Context context)
				throws IOException, InterruptedException {
			String[] inputSplit;
			double pageRank;
			String adjacencyListString = "";

			// each line of the text follows the format (page, [outlinks], PageRank)
			String[] tokens = value.toString().split(LINE_DELIMITER);

			// takes in the input text file and calculate the total number of vertices
			// and the dangling page PageRank mass sum.
			for (int i = 0; i < tokens.length; i++) {
				inputSplit = tokens[i].split(COMMA_DELIMITER);
				adjacencyListString = inputSplit[1].replace("[", "").replace("]", "");
				pageRank = Double.parseDouble(inputSplit[2]);

				if (adjacencyListString.isEmpty()) {
					context.getCounter("counter", "PR_MASS_SUM").increment((long) (pageRank * 1000000000));
				}
				context.getCounter("counter", "TOTAL_NUM_VERTICES").increment(new Long(1));
			}
		}
	}

	// also takes in the input text file and emit pair (vertex ID, Vertex object).
	// each Vertex object contains its ID, pageRank, adjacencyList, and boolean
	// value flag (dangling page ? true : else false).
	// emit twice, one with key vertex ID, and the another one with key inlink ID.
	public static class PageRankMapper extends Mapper<Object, Text, IntWritable, Vertex> {
		final String LINE_DELIMITER = "\n";
		final String COMMA_DELIMITER = ",";

		@Override
		public void map(final Object key, final Text value, final Context context)
				throws IOException, InterruptedException {
			String[] inputSplit;
			String adjacencyListString = "";
			int vertexId;
			int inlinkId;
			double pageRank;

			String[] tokens = value.toString().split(LINE_DELIMITER);

			// get vertex, adjacency list, and PageRank by unpacking the tokens
			for (int i = 0; i < tokens.length; i++) {
				inputSplit = tokens[i].split(COMMA_DELIMITER);
				vertexId = Integer.parseInt(inputSplit[0]);
				adjacencyListString = inputSplit[1].replace("[", "").replace("]", "");
				pageRank = Double.parseDouble(inputSplit[2]);

				// check if the vertex has no adjacency list (dangling page)
				if (!adjacencyListString.isEmpty()) {
					inlinkId = Integer.parseInt(adjacencyListString);
					// emit pair (key: inlink ID, object: vertex)
					context.write(new IntWritable(inlinkId),
							new Vertex(vertexId, pageRank, adjacencyListString, false));
				}
				// emit pair (key: vertex ID, object: vertex)
				context.write(new IntWritable(vertexId), new Vertex(vertexId, pageRank, adjacencyListString, true));
			}
		}
	}

	public static class PageRankReducer extends Reducer<IntWritable, Vertex, IntWritable, Text> {
		private final Text word = new Text();
		private final double alpha = 0.85;
		private double newPageRank = 0;
		private double danglingPageRankSum;
		private int totalNumVertices;
		private String adjacencyListString;
		private boolean isFinalIteration;

		@Override
		public void setup(Context context) {
			// retrieve value from the global counters which will help calculate and
			// update the new PageRank for each page

			danglingPageRankSum = Double.parseDouble(context.getConfiguration().get("PRMassSum")) / 1000000000;
			totalNumVertices = Integer.parseInt(context.getConfiguration().get("TotalNumVertices"));
			isFinalIteration = context.getConfiguration().getBoolean("IsFinalIteration", false);
		}

		@Override
		public void reduce(final IntWritable vertexId, final Iterable<Vertex> vertices, final Context context)
				throws IOException, InterruptedException {

			adjacencyListString = "[]";

			newPageRank = ((1 - alpha) / totalNumVertices) + (alpha * danglingPageRankSum / totalNumVertices);

			// iterate through object vertices.
			// for each vertex, if flag shows false (not a dangling page), calculate the
			// final PageRank.
			// get the page's true adjacency list.
			for (Vertex vertex : vertices) {

				if (!vertex.handleDanglingPage()) {
					newPageRank += alpha * vertex.getPageRank();
				} else {
					adjacencyListString = "[" + vertex.getAdjacencyListString() + "]";
				}
			}

			// if this run is the final iteration, filter out the values with vertex ID
			// greater than 19 to only get the values with vertex ID 0 ~ 19.
			if (isFinalIteration) {
				if (vertexId.get() > 19) {
					return;
				}
			}

			// here the reducer output has same format as the input, since this output
			// should become the input for the mapper in next iteration
			word.set(adjacencyListString + "," + newPageRank);
			context.write(vertexId, word);
		}
	}

	@Override
	public int run(final String[] args) throws Exception {
		boolean isFirstIteration = true;
		int numIterations = 10;

		// chained multiple jobs iterations
		for (int iter = 1; iter < numIterations + 1; iter++) {
			Configuration conf = getConf();
			Job job1 = Job.getInstance(conf, "PageRank");
			job1.setJarByClass(PageRank.class);

			job1.setMapOutputKeyClass(IntWritable.class);
			job1.setMapOutputValueClass(Vertex.class);

			job1.setMapperClass(DanglingPageMapper.class);

			if (isFirstIteration) {
				FileInputFormat.addInputPath(job1, new Path(args[0]));
			} else {
				FileInputFormat.addInputPath(job1, new Path(args[1] + "/iter" + (iter - 1)));
			}
			FileOutputFormat.setOutputPath(job1, new Path("dummy" + "/dummy" + (iter - 1)));

			job1.waitForCompletion(true);

			long massSum = (long) job1.getCounters().findCounter("counter", "PR_MASS_SUM").getValue();
			long totalNumVertices = (long) job1.getCounters().findCounter("counter", "TOTAL_NUM_VERTICES").getValue();

			Configuration conf2 = getConf();
			Job job2 = Job.getInstance(conf2, "PageRank");
			job2.setJarByClass(PageRank.class);
			Configuration jobConf2 = job2.getConfiguration();

			jobConf2.set("mapreduce.output.textoutputformat.separator", ",");
			jobConf2.setLong("PRMassSum", massSum);
			jobConf2.setLong("TotalNumVertices", totalNumVertices);

			if (iter == numIterations) {
				jobConf2.setBoolean("IsFinalIteration", true);
			} else {
				jobConf2.setBoolean("IsFinalIteration", false);
			}

			job2.setMapOutputKeyClass(IntWritable.class);
			job2.setMapOutputValueClass(Vertex.class);

			job2.setMapperClass(PageRankMapper.class);
			job2.setReducerClass(PageRankReducer.class);

			if (isFirstIteration) {
				FileInputFormat.addInputPath(job2, new Path(args[0]));
				isFirstIteration = false;
			} else {
				FileInputFormat.addInputPath(job2, new Path(args[1] + "/iter" + (iter - 1)));
			}
			FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/iter" + iter));

			job2.waitForCompletion(true);
		}

		return 0;
	}

	public static void main(final String[] args) {
		if (args.length != 2) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}

		try {
			ToolRunner.run(new PageRank(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}

class Vertex implements Writable {

	private int vertexId = 0;
	private double pageRank = 0;
	private String adjacencyListString;
	private boolean handleDanglingPage = true;

	Vertex() {
	}

	Vertex(int vertexId, double pageRank, String adjacencyListString, boolean handleDanglingPage) {
		this.vertexId = vertexId;
		this.pageRank = pageRank;
		this.adjacencyListString = adjacencyListString;
		this.handleDanglingPage = handleDanglingPage;
	}

	public int getVertexId() {
		return this.vertexId;
	}

	public boolean handleDanglingPage() {
		return this.handleDanglingPage;
	}

	public double getPageRank() {
		return this.pageRank;
	}

	public String getAdjacencyListString() {
		return this.adjacencyListString;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(vertexId);
		out.writeDouble(pageRank);
		out.writeUTF(adjacencyListString);
		out.writeBoolean(handleDanglingPage);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.vertexId = in.readInt();
		this.pageRank = in.readDouble();
		this.adjacencyListString = in.readUTF();
		this.handleDanglingPage = in.readBoolean();
	}
}