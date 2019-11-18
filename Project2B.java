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

/**
    Note that this code is derived from the adjacency list example posted on blackboard.
    The most significant changes are to the map function
 */

public class Project2B {

    public static class adjMapper extends Mapper<Object, Text, Text, Text>
    {

        private Pair<Integer, String> directedMax, directedMin, undirectedMax, undirectedMin;
        private Pair<String, String> directedLongestAdjList, undirectedLongestAdjList;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
        {
            String input = value.toString();
            if (!input.startsWith("#"))
            {
                // input is formatted as "undir_nodeId  neighborNode,neighborNode"
                String[] inputList = input.split("\t");
                String[] nodeIdStr = inputList[0].split("_");
                String nodeId = nodeIdStr[1];
                String graphType = nodeIdStr[0];
                boolean directed = graphType.equals("dir");
                String key = directed ? "directed" : "undirected"
                String[] adjNodes = inputList[1].split(",");
                
                int numAdjNodes = adjNodes.length;

                if (directed) {
                    if(directedMax == null || numAdjNodes > directedMax.value.intValue()) {
                        directedMax = new Pair(nodeId, numAdjNodes);
                        directedLongestAdjList = new Pair(nodeId, inputList[1]);
                    }
                    if(directedMin == null || numAdjNodes < directedMin.value.intValue()) {
                        directedMin = new Pair(nodeId, numAdjNodes);
                    }
                }
                else {
                    if(undirectedMax == null || numAdjNodes > undirectedMax.value.intValue()) {
                        undirectedMax = new Pair(nodeId, numAdjNodes);
                        undirectedLongestAdjList = new Pair(nodeId, inputList[1]);
                    }
                    if(undirectedMin == null || numAdjNodes < undirectedMin.value.intValue()) {
                        undirectedMin = new Pair(nodeId, numAdjNodes);
                    }
                }

                //context.write(toText(key), toText(nodeId + "||" + inputList[1] + '||' + numAdjNodes));
            }
        }

        public void cleanup(Context c) {
            c.write("undirected_min_connections", toText(undirectedMin.toString()));
            c.write("undirected_max_connections", toText(undirectedMax.toString()));
            c.write("directed_min_connections", toText(directedMin.toString()));
            c.write("directed_max_connections", toText(directedMax.toString()));
            c.write("undirected_longest_adj", toText(directedLongestAdjList.toString()));
            c.write("directed_longest_adj", toText(undirectedLongestAdjList.toString()));
        }

        public Text toText(String s) {
            Text t = new Text();
            t.set(s);
            return t;
        }

        private class Pair<K, V> {
            public K key;
            public V value;

            public Pair(K key, V value) {
                this.key = key;
                this.value = value;
            }

            public String toString() {
                return key.toString() + "||" + value.toString();
            }
        }
    }

    public static class adjReducer extends Reducer<Text,Text,Text, Text> 
    {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values,
                        Context context
                        ) throws IOException, InterruptedException 
        {
            String result;
            int resultValue;
            for (Text v : values) {
                String[] pair = v.toString().split("||");

                if (key.contains("min_conn")) {
                    int val = Integer.parseInt(pair[1]);
                    if (val < resultValue) {
                        result = "" + val;
                        resultValue = val;
                    }
                }
                else if (key.contains("max_conn")) {
                    int val = Integer.parseInt(pair[1]);
                    if (val > resultValue) {
                        result = "" + val;
                        resultValue = val;
                    }
                }
                else if (key.contains("longest")){
                    int val = (pair[1].split(",").length);
                    if (val > resultValue) {
                        resultValue = val;
                        result = v;
                    }
                }
            }
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception 
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Cloud Computing Adj List");
        job.setJarByClass(Project2.class);
        job.setMapperClass(adjMapper.class);
        job.setCombinerClass(adjReducer.class);
        job.setReducerClass(adjReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}