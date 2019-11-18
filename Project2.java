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

public class Project2 {

    public static class adjMapper extends Mapper<Object, Text, Text, Text>
    {

        //private final static IntWritable one = new IntWritable(1);
        private Text outVal = new Text();
        private Text directedOutputKey = new Text();
        private Text undirectedOutputKey = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
        {
            System.out.println(value.toString());
            String inline = value.toString();
            if (!inline.startsWith("#"))
            {
                String[] inVals = inline.split("\t").split(" ");
                String originNodeId = inVals[0];
                String targetNodeId = inVals[1];

                context.write(toText(("dir_" + originNodeId)), toText(targetNodeId));

                context.write(toText(("undir_" + originNodeId)), toText(targetNodeId));
                context.write(toText(("undir_" + targetNodeId)), toText(originNodeId));
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
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values,
                        Context context
                        ) throws IOException, InterruptedException 
        {
            int numAdjNodes = 0;
            String adjList = "";
            for (Text val : values)
            {
                adjList = adjList + "," + val;
                numAdjNodes++;
            }
            adjList = adjList + "|" + numAdjNodes;
            result.set(adjList);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception 
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Cloud Computing Adj List");
        job.setJarByClass(adjList.class);
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
