package com.hxf.hdfs.utils;

/**
 * Created by fangqing on 12/25/17.
 */


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

public class TopKApp {
    static final String INPUT_PATH = "hdfs://localhost:9000/input2";
    static final String OUT_PATH = "hdfs://localhost:9000/out2";

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        final FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
        final Path outPath = new Path(OUT_PATH);
        if (fileSystem.exists(outPath)) {
            fileSystem.delete(outPath, true);
        }

        final Job job = new Job(conf, "apptopk");
        FileInputFormat.setInputPaths(job, INPUT_PATH);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(NullWritable.class);
        FileOutputFormat.setOutputPath(job, outPath);
        job.waitForCompletion(true);
    }

    static class MyMapper extends Mapper<LongWritable, Text, LongWritable, NullWritable> {
        long max = Long.MIN_VALUE;

        protected void map(LongWritable k1, Text v1, Context context) throws java.io.IOException, InterruptedException {
            final long temp = Long.parseLong(v1.toString());
            if (temp > max) {
                max = temp;
            }
        }



        protected void cleanup(org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, LongWritable, NullWritable>.Context context) throws java.io.IOException, InterruptedException {
            context.write(new LongWritable(max), NullWritable.get());
        }


    }

    static class MyReducer extends Reducer<LongWritable, NullWritable, LongWritable, NullWritable> {
        long max = Long.MIN_VALUE;

        protected void reduce(LongWritable k2, java.lang.Iterable<NullWritable> arg1, org.apache.hadoop.mapreduce.Reducer<LongWritable, NullWritable, LongWritable, NullWritable>.Context arg2) throws java.io.IOException, InterruptedException {
            final long temp = k2.get();
            if (temp > max) {
                max = temp;
            }
        }



        protected void cleanup(org.apache.hadoop.mapreduce.Reducer<LongWritable, NullWritable, LongWritable, NullWritable>.Context context) throws java.io.IOException, InterruptedException {
            context.write(new LongWritable(max), NullWritable.get());
        }


    }
}
