package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class WordCountRemote
{
    private static final Logger LOG = LogManager.getLogger(WordCountRemote.class);
    public static class TokenizerMapper
        extends Mapper<Object, Text, Text, IntWritable>{
        // KEYIN,VALUEIN,KEYOUT,VALUEOUT

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
                        ) throws IOException, InterruptedException {

            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {
        // KEYIN,VALUEIN,KEYOUT,VALUEOUT
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
                            ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
    public static void main( String[] args ) {
        String appName = WordCountRemote.class.toString();

        // Init job client
        Configuration conf = new Configuration();

        // Resource manager address port
        conf.set("yarn.resourcemanager.address", System.getenv("REMOTE_RM_ADDRESS"));

        // Submit job to yarn resource manager
        conf.set("mapreduce.framework.name", "yarn");

        // Namenode address port
        conf.set("fs.defaultFS", System.getenv("REMOTE_NN_ADDRESS"));

        try {
            Job job = Job.getInstance(conf, WordCountRemote.class.toString());
            job.setJarByClass(WordCountRemote.class);
            job.setMapperClass(TokenizerMapper.class);

            // Agg per map
            job.setCombinerClass(IntSumReducer.class);

            // Agg across whole maps
            job.setReducerClass(IntSumReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job, new Path(args[1]));
            FileOutputFormat.setOutputPath(job, new Path(args[2]));

            job.submit();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}