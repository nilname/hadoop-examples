package com.hxf.hdfs.utils;

/**
 * Created by fangqing on 12/25/17.
 */

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class SortAppV1 {
    static final String INPUT_PATH = "hdfs://localhost:9000/newinput";
    static final String OUT_PATH = "hdfs://localhost:9000/newoutput";

    public static void main(String[] args) throws Exception {
        final Configuration configuration = new Configuration();
        final FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), configuration);
        if (fileSystem.exists(new Path(OUT_PATH))) {
            fileSystem.delete(new Path(OUT_PATH), true);
        }
        final Job job = new Job(configuration, SortApp.class.getSimpleName());
        //1.1 指定输入文件路径
        FileInputFormat.setInputPaths(job, INPUT_PATH);
        job.setInputFormatClass(TextInputFormat.class);//指定哪个类用来格式化输入文件

        //1.2指定自定义的Mapper类
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(NewK2.class);//指定输出<k2,v2>的类型
        job.setMapOutputValueClass(LongWritable.class);

        //1.3 指定分区类
        job.setPartitionerClass(HashPartitioner.class);
        job.setNumReduceTasks(1);

        //2.2 指定自定义的reduce类
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(LongWritable.class);//指定输出<k3,v3>的类型
        job.setOutputValueClass(LongWritable.class);

        //2.3 指定输出到哪里
        FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
        job.setOutputFormatClass(TextOutputFormat.class);//设定输出文件的格式化类
        job.waitForCompletion(true);//把代码提交给JobTracker执行
    }


    static class MyMapper extends Mapper<LongWritable, Text, NewK2, LongWritable> {
        protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, NewK2, LongWritable>.Context context) throws java.io.IOException, InterruptedException {
            final String[] splited = value.toString().split("\t");
            final NewK2 k2 = new NewK2(Long.parseLong(splited[0]), Long.parseLong(splited[1]));
            final LongWritable v2 = new LongWritable(Long.parseLong(splited[1]));
            context.write(k2, v2);
        }
    }

    static class MyReducer extends Reducer<NewK2, LongWritable, LongWritable, LongWritable> {
        protected void reduce(NewK2 k2, java.lang.Iterable<LongWritable> v2s, org.apache.hadoop.mapreduce.Reducer<NewK2, LongWritable, LongWritable, LongWritable>.Context context) throws java.io.IOException, InterruptedException {
            context.write(new LongWritable(k2.first), new LongWritable(k2.second));
        }
    }

    /**
     * 问：为什么实现该类？
     * 答：因为原来的v2不能参与排序，把原来的k2和v2封装到一个类中，作为新的k2
     */
    static class NewK2 implements WritableComparable<NewK2> {
        Long first;
        Long second;

        public NewK2() {
        }

        public NewK2(long first, long second) {
            this.first = first;
            this.second = second;
        }


        //        @Override
        public void readFields(DataInput in) throws IOException {
            this.first = in.readLong();
            this.second = in.readLong();
        }

        //        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(first);
            out.writeLong(second);
        }

        /**
         * 当k2进行排序时，会调用该方法.
         * 当第一列不同时，升序；当第一列相同时，第二列升序
         */
//        @Override
        public int compareTo(NewK2 o) {
            final long minus = this.first - o.first;
            if (minus != 0) {
                return (int) minus;
            }
            return (int) (this.second - o.second);
        }

        @Override
        public int hashCode() {
            return this.first.hashCode() + this.second.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof NewK2)) {
                return false;
            }
            NewK2 oK2 = (NewK2) obj;
            return (this.first == oK2.first) && (this.second == oK2.second);
        }
    }

}

/*
我们的新类型NewK2实现了WritableComparable接口，其中该接口中有一个compareTo()方法，
当对关键字进行比较会调用该方法，而我们就在该方法中实现了我们想要做的事

1    1
2    1
2    2
3    1
3    2
3    3
 */
