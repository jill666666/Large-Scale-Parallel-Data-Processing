package wc;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class ReplicatedJoin extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(ReplicatedJoin.class);
    private static final String LINE_DELIMITER = "\n";
    private static final String COMMA_DELIMITER = ",";

    public static class RepJoinMapper extends Mapper<Object, Text, Text, Text> {
        private final Text keyText = new Text();
        private final Text valueText = new Text();
        private HashMap<Integer, List<Integer>> edgeMap = new HashMap<Integer, List<Integer>>();
        final int MAX_VALUE = 12000;

        @Override
        public void setup(Context context) {

            // get the input path config from the context
            String inputPath = context.getConfiguration().get("input.path");

            try {
                // read from the file cache
                // BufferedReader rdr = new BufferedReader(new FileReader(inputPath + "/edges.csv"));
                // BufferedReader rdr = new BufferedReader(new FileReader(("s3://hw3-spark-bucket/input/edges.csv")));

                URI s3uri = new URI("s3://hw3-spark-bucket");
                Configuration conf = new Configuration();
                FileSystem fs = FileSystem.get(s3uri, conf);
                Path inFile = new Path("/input/edges.csv");

                BufferedReader rdr = new BufferedReader(new InputStreamReader(fs.open(inFile)));

                // new InputStreamReader((fs.open(path)))

                String line;
                // read line by line and split the edge to get the nodes
                while ((line = rdr.readLine()) != null) {

                    String[] nodes = line.split(COMMA_DELIMITER);
                    int from = Integer.parseInt(nodes[0]);
                    int to = Integer.parseInt(nodes[1]);

                    // filter out the value that is equal to or above max value 
                    if (from < MAX_VALUE && to < MAX_VALUE) {
                        // update the hashmap where the key is 'from' node and the value is 'to' node
                        // this hashmap is further used to calculate the triangle count
                        if (edgeMap.containsKey(from)) {
                            edgeMap.get(from).add(to);
                            // logger.info(from + " -> " + to + " >>> " + edgeMap.get(from));
                        } else {
                            List<Integer> intList = new ArrayList<Integer>();
                            intList.add(to);
                            edgeMap.put(from, intList);
                            // logger.info(from + " -> " + to + " >>> " + edgeMap.get(from));
                        }
                    }
                }

                rdr.close();

            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (URISyntaxException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        @Override
        public void map(final Object key, final Text value, final Context context)
                throws IOException, InterruptedException {

            String[] tokens = value.toString().split(LINE_DELIMITER);

            // iterate through the tokens to get the 'from' and 'to' nodes
            for (int i = 0; i < tokens.length; i++) {
                String[] nodes = tokens[i].split(COMMA_DELIMITER);
                int from = Integer.parseInt(nodes[0]);
                int to = Integer.parseInt(nodes[1]);

                // check if the given nodes can form the triangle using the hashmap lookup
                // if found, increment the global counter and emit the result
                if (from < MAX_VALUE && to < MAX_VALUE) {
                    if (edgeMap.get(to) != null) {
                        // logger.info(from + " -> " + to + " >>> " + edgeMap.get(to));
                        for (int joinNode : edgeMap.get(to)) {
                            if (edgeMap.get(joinNode) != null && edgeMap.get(joinNode).contains(from)) {
                                context.getCounter("counter", "NUM_OF_TRIANGLES").increment(new Long(1));
                                // }
                                keyText.set(String.valueOf(from));
                                valueText.set(String.valueOf(joinNode) + "," + String.valueOf(to));
                                context.write(keyText, valueText);
                            }
                        }
                    }
                }
            }
        }
    }

    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = getConf();
        final Job job = Job.getInstance(conf, "Replicated Join");
        job.setJarByClass(ReplicatedJoin.class);
        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", ",");
        // Delete output directory, only to ease local development; will not work on
        // AWS. ===========
        // final FileSystem fileSystem = FileSystem.get(conf);
        // if (fileSystem.exists(new Path(args[1]))) {
        // fileSystem.delete(new Path(args[1]), true);
        // }
        // ================

        jobConf.set("join.type", "inner");
        jobConf.set("input.path", args[0].toString());

        job.setMapperClass(RepJoinMapper.class);
        // job.setReducerClass(RepJoinReducer.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // configure the DistributedCache
        // DistributedCache.addCacheFile(new Path(args[0]).toUri(),
        // job.getConfiguration());
        // DistributedCache.setLocalFiles(job.getConfiguration(), args[0]);

        // job.addCacheFile(new Path(args[0]).toUri());
        // job.addCacheFile(new URI(args[0]));

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        if (job.waitForCompletion(true)) {
            // after the reducers job has ended,
            // get the global counter value which represents number of output edges
            long numOfOutputEdges = job.getCounters().findCounter("counter", "NUM_OF_TRIANGLES").getValue();
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
        if (args.length != 2) {
            throw new Error("Two arguments required:\n<input-dir> <output-dir>");
        }

        try {
            ToolRunner.run(new ReplicatedJoin(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }

}