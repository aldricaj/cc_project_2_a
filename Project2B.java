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

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
        {
            if (value != null) {
                String input = value.toString();
                if (input != null && !input.startsWith("#"))
                {
                    // input is formatted as "undir_nodeId  neighborNode,neighborNode"
                    String[] inputList = input.split("\t");
                    String[] nodeIdStr = inputList[0].split("_");
                    String nodeId = nodeIdStr[1];
                    String graphType = nodeIdStr[0];
                    boolean directed = graphType.equals("dir");
                    String graphTypeText = directed ? "directed" : "undirected";
                    String[] adjNodes = inputList[1].split(",");
                    
                    int numAdjNodes = adjNodes.length;

                    context.write(toText(graphTypeText), toText(nodeId + "\t" + inputList[1] + "\t" + numAdjNodes));
                    
                }
            }
            
        }
        public Text toText(String s) {
            Text t = new Text();
            t.set(s);
            return t;
        }

    }

    public static class adjReducer extends Reducer<Text,Text,Text, Text> 
    {
        private Statistics directedStats = new Statistics("directed");
        private Statistics undirectedStats = new Statistics("undirected");

        public void reduce(Text key, Iterable<Text> values,
                        Context context
                        ) throws IOException, InterruptedException 
        {
            String result = "";
            int resultValue = -1;
            String k = key.toString();
            Statistics statCollector = (k == "directed") ? directedStats : undirectedStats;
            for (Text v : values) {
                Row r = new Row(v.toString());
                if (r.length > statCollector.maxConnectivity) {
                    statCollector.maxConnectivity = r.length;
                    statCollector.longestAdjList = r.id + ":" + r.adjList;
                }

                if (r.length < statCollector.minConnectivity) {
                    statCollector.minConnectivity = r.length;
                }
            }
            
        }
        public void cleanup(Context c) throws IOException, InterruptedException{
            directedStats.AddToContext(c);
            undirectedStats.AddToContext(c);
        }
        
        private class Statistics {
            public int maxConnectivity = -1;
            public int minConnectivity = 9_000_000;
            public String longestAdjList = "";
            public String graphType;

            public Statistics(String type) {
                graphType = type;
            }

            public void AddToContext(Context c) {
                c.write(toText(graphType + "_max_connectivity:"), toText(minConnectivity + ""));
                c.write(toText(graphType + "_min_connectivity:"), toText(maxConnectivity + ""));
                c.write(toText(graphType + "_longestAdjList:"), toText(longestAdjList.toString()));
            }
            private Text toText(String s) {
            Text t = new Text();
            t.set(s);
            return t;
        }
        }
        private class Row {
            public String adjList;
            public String id;
            public int length;
            public Row(String s) {
                
                System.out.println(s);
                String[] vars = s.split("\t");
                System.out.println(vars.length);
                id = vars[0];
                adjList = vars[1];
                System.out.println(vars[0]);
                System.out.println(vars[1]);
                System.out.println(vars[2]);
                length = Integer.parseInt(vars[2]);
                
            }
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
