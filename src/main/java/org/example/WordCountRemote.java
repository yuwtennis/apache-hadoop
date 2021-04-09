package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
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
//        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        conf.set("yarn.resourcemanager.address", "192.168.11.20:8032");
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        // Start yarn client
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();

        // Set up application context
        try {
            // First create application
            YarnClientApplication app = yarnClient.createApplication();
            GetNewApplicationResponse appResponse = app.getNewApplicationResponse();

            LOG.info("Initialized application. AppId: %s , Available Resources:%s",
                    appResponse.getApplicationId().toString(),
                    appResponse.getMaximumResourceCapability().toString());

            // Set application (submission) context and prepare application container containing ApplicationMaster
            ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
            ApplicationId appId = appContext.getApplicationId();

            appContext.setKeepContainersAcrossApplicationAttempts(true);
            appContext.setApplicationName(appName);

            Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

            LOG.info("Copy App Master jar from local filesystem and add to local environment.");
        } catch (YarnException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
