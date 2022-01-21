package wc;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class ReduceSideJoin extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(ReduceSideJoin.class);
    private static final String LINE_DELIMITER = "\n";
    private static final String COMMA_DELIMITER = ",";

    public static class InputMapper extends Mapper<Object, Text, Text, Text> {
        private final Text keyText = new Text();
        private final Text valueText = new Text();
        final int MAX_VALUE = 12000;

        @Override
        public void map(final Object key, final Text value, final Context context)
                throws IOException, InterruptedException {

            // given the input string, split by whitespace "\n" and store each of the line
            String[] tokens = value.toString().split(LINE_DELIMITER);

            // iterate through the tokens to get the 'from' and 'to' nodes
            for (int i = 0; i < tokens.length; i++) {
                String[] nodes = tokens[i].split(COMMA_DELIMITER);
                String from = nodes[0];
                String to = nodes[1];

                if (Integer.parseInt(from) < MAX_VALUE && Integer.parseInt(to) < MAX_VALUE) {
                    // write context where key is source node and value is destination node
                    // concatenated with "I" ("Input")
                    // ex. for edge (1, 2) --> emit ("1", "I2")
                    keyText.set(from.toString());
                    valueText.set("I" + to.toString());
                    context.write(keyText, valueText);
                }
            }
        }
    }

    public static class Path2Mapper extends Mapper<Object, Text, Text, Text> {
        private final Text keyText = new Text();
        private final Text valueText = new Text();

        @Override
        public void map(final Object key, final Text value, final Context context)
                throws IOException, InterruptedException {

            // given the input string, split by whitespace "\n" and store each of the line
            String[] tokens = value.toString().split(LINE_DELIMITER);

            // iterate through the tokens to get the 'from' and 'to' nodes
            for (int i = 0; i < tokens.length; i++) {
                String[] nodes = tokens[i].split(COMMA_DELIMITER);
                String from = nodes[0];
                String to = nodes[1];

                // write context where key is source node and value is destination node
                // concatenated with "P" ("Path2")
                // ex. for edge (1, 2) --> emit ("1", "P2")
                keyText.set(from.toString());
                valueText.set("P" + to.toString());
                context.write(keyText, valueText);
            }
        }
    }

    public static class Path2Reducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(final Text key, final Iterable<Text> values, final Context context)
                throws IOException, InterruptedException {

            ArrayList<String> inputArray = new ArrayList<String>();
            ArrayList<String> path2Array = new ArrayList<String>();

            // divide the node by flag and add accordingly to the array
            for (Text v : values) {
                String node = v.toString().substring(1);
                String flag = v.toString().substring(0, 1);
                if (flag.equals("I")) {
                    inputArray.add(node);
                } else {
                    path2Array.add(node);
                }
            }

            // if the pair matches (edge can "close the triangle"), emit the result
            // and increment the global counter.
            // counter represents number of output edges.
            // the counter value is divided by 3 to remove the duplicate triangle counts
            if (inputArray.size() != 0 && path2Array.size() != 0) {
                for (String input : inputArray) {
                    for (String path2 : path2Array) {
                        if (input.equals(path2)) {
                            context.write(key, new Text(input));
                            context.getCounter("counter", "NUM_OF_OUTPUT_EDGES").increment(new Long(1));
                        }
                    }
                }
            }
        }
    }

    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = getConf();
        final Job job = Job.getInstance(conf, "ReduceSide Join");
        job.setJarByClass(ReduceSideJoin.class);
        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", ",");
        // Delete output directory, only to ease local development; will not work on
        // AWS. ===========
        // final FileSystem fileSystem = FileSystem.get(conf);
        // if (fileSystem.exists(new Path(args[1]))) {
        // fileSystem.delete(new Path(args[1]), true);
        // }
        // ================

        // job.setMapperClass(Path2Mapper.class);

        job.setReducerClass(Path2Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // FileInputFormat.addInputPath(job, new Path(args[0]));
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, InputMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Path2Mapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        if (job.waitForCompletion(true)) {
            // after the reducers job has ended,
            // get the global counter value which represents number of output edges
            long numOfOutputEdges = job.getCounters().findCounter("counter", "NUM_OF_OUTPUT_EDGES").getValue();
            // calculate the final number of triangles.
            // remove duplicates by dividing the number of output edges by 3
            long numOfTriangles = numOfOutputEdges / 3;
            logger.info("num of output edges: " + numOfOutputEdges);
            logger.info("num of triangles: " + numOfTriangles);
            return 0;
        }
        return 1;
    }

    public static void main(final String[] args) {
        if (args.length != 3) {
            throw new Error("Three arguments required:\n<input-dir> <path2-dir> <output-dir>");
        }

        try {
            ToolRunner.run(new ReduceSideJoin(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }

}