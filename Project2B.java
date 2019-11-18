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

        private Pair<String, Integer> directedMax, directedMin, undirectedMax, undirectedMin;
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
                //String graphType = directed ? "directed" : "undirected";
                String[] adjNodes = inputList[1].split(",");
                
                int numAdjNodes = adjNodes.length;

                if (directed) {
                    Integer directedMaxVal = directedMax.value;
                    if(directedMax == null || numAdjNodes > directedMaxVal.intValue()) {
                        directedMax = new Pair(nodeId, numAdjNodes);
                        directedLongestAdjList = new Pair(nodeId, inputList[1]);
                    }
                    Integer directedMinVal = directedMin.value;
                    if(directedMin == null || numAdjNodes < directedMinVal.intValue()) {
                        directedMin = new Pair(nodeId, numAdjNodes);
                    }
                }
                else {
                    Integer undirectedMaxVal = undirectedMax.value;
                    if(undirectedMax == null || numAdjNodes > undirectedMaxVal.intValue()) {
                        undirectedMax = new Pair(nodeId, numAdjNodes);
                        undirectedLongestAdjList = new Pair(nodeId, inputList[1]);
                    }
                    Integer undirectedMinVal = undirectedMin.value;
                    if(undirectedMin == null || numAdjNodes < undirectedMinVal.intValue()) {
                        undirectedMin = new Pair(nodeId, numAdjNodes);
                    }
                }

                //context.write(toText(key), toText(nodeId + "||" + inputList[1] + '||' + numAdjNodes));
            }
        }

        public void cleanup(Context c) throws IOException, InterruptedException {
            if (!undirectedMin == null)
                c.write(new Text("undirected_min_connections"), toText(undirectedMin.toString()));
            if (!undirectedMax == null)
                c.write(new Text("undirected_max_connections"), toText(undirectedMax.toString()));
            if (!directedMin == null)
                c.write(new Text("directed_min_connections"), toText(directedMin.toString()));
            if (!directedMax == null)
                c.write(new Text("directed_max_connections"), toText(directedMax.toString()));
            if (!directedLongestAdjList == null)
                c.write(new Text("undirected_longest_adj"), toText(directedLongestAdjList.toString()));
            if (!undirectedLongestAdjList == null)
                c.write(new Text("directed_longest_adj"), toText(undirectedLongestAdjList.toString()));
            
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
            String result = "";
            int resultValue = -1;
            String k = key.toString();
            for (Text v : values) {
                String[] pair = v.toString().split("||");

                if (k.contains("min_conn")) {
                    int val = Integer.parseInt(pair[1]);
                    if (resultValue == -1 || val < resultValue) {
                        result = "" + val;
                        resultValue = val;
                    }
                }
                else if (k.contains("max_conn")) {
                    int val = Integer.parseInt(pair[1]);
                    if (resultValue == -1 || val > resultValue) {
                        result = "" + val;
                        resultValue = val;
                    }
                }
                else if (k.contains("longest")){
                    int val = (pair[1].split(",").length);
                    if (resultValue == -1 || val > resultValue) {
                        resultValue = val;
                        result = v.toString();
                    }
                }
            }
            context.write(key, toText(result));
        }
        public Text toText(String s) {
            Text t = new Text();
            t.set(s);
            return t;
        }
    }

    public static void main(String[] args) throws Exception 
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Cloud Computing Adj List");
        job.setJarByClass(Project2B.class);
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
