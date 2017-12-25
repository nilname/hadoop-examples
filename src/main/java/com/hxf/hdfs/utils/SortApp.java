package com.hxf.hdfs.utils;

/**
 * Created by fangqing on 12/25/17.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.IOException;
import java.net.URI;

public class SortApp {
    private static final String INPUT_PATH = "hdfs://localhost:9000/input";
    private static final String OUT_PATH = "hdfs://localhost:9000/output";
    public static void main(String[] args) throws Exception {
        Configuration conf=new Configuration();
        final FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
        final Path outpath = new Path(OUT_PATH);
        if(fileSystem.exists(outpath)){
            fileSystem.delete(outpath,true);
        }

        final Job job = new Job(conf,SortApp.class.getSimpleName());

        //1.1 指定输入文件路径
        FileInputFormat.setInputPaths(job, INPUT_PATH);
        job.setInputFormatClass(TextInputFormat.class);//指定哪个类用来格式化输入文件

        //1.2指定自定义的Mapper类
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);//指定输出<k2,v2>的类型
        job.setMapOutputValueClass(LongWritable.class);

        //1.3 指定分区类
        job.setPartitionerClass(HashPartitioner.class);
        job.setNumReduceTasks(1);

        //1.4 TODO 排序、分区

        //1.5  TODO （可选）合并

        //2.2 指定自定义的reduce类
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(LongWritable.class);//指定输出<k3,v3>的类型
        job.setOutputValueClass(LongWritable.class);

        //2.3 指定输出到哪里
        FileOutputFormat.setOutputPath(job, outpath);
        job.setOutputFormatClass(TextOutputFormat.class);//设定输出文件的格式化类
        job.waitForCompletion(true);//把代码提交给JobTracker执行
    }
    static class MyMapper extends Mapper<LongWritable, Text,LongWritable,LongWritable>{

        @Override
        protected void map(
                LongWritable key,
                Text value,
                Mapper<LongWritable, Text, LongWritable, LongWritable>.Context context)
                throws IOException, InterruptedException {
            final String[] splited = value.toString().split("\t");
            final long k2 = Long.parseLong(splited[0]);
            final long v2 = Long.parseLong(splited[1]);
            context.write(new LongWritable(k2),new LongWritable(v2));
        }
    }
    static class MyReducer extends Reducer<LongWritable,LongWritable,LongWritable,LongWritable>{

        @Override
        protected void reduce(
                LongWritable k2,
                Iterable<LongWritable> v2s,
                Reducer<LongWritable, LongWritable, LongWritable, LongWritable>.Context context)
                throws IOException, InterruptedException {
            for(LongWritable v2:v2s){
                context.write(k2, v2);
            }
        }
    }
}


/*


我们知道排序分组是MapReduce中Mapper端的第四步，其中分组排序都是基于Key的，我们可以通过下面这几个例子来体现出来。其中的数据和任务如下图1.1，1.2所示。

复制代码
#首先按照第一列升序排列，当第一列相同时，第二列升序排列
3    3
3    2
3    1
2    2
2    1
1    1
-------------------
#结果
1    1
2    1
2    2
3    1
3    2
3    3
复制代码
图 1.1 排序

复制代码
#当第一列相同时，求出第二列的最小值
3    3
3    2
3    1
2    2
2    1
1    1
-------------------
#结果
3    1
2    1
1    1


----------
默认排序
1    1
2    2
2    1
3    3
3    2
3    1

运行结果可以看出，MapReduce默认排序算法只对Key进行了排序，并没有对value进行排序，
没有达到我们的要求，所以要实现我们的要求，还要我们自定义一个排序算法

从上面图中运行结果可以知道，MapReduce默认排序算法只对Key进行了排序，并没有对value进行排序，
没有达到我们的要求，所以要实现我们的要求，还要我们自定义一个排序算法。在map和reduce阶段进行排序时，
比较的是k2。v2是不参与排序比较的。如果要想让v2也进行排序，需要把k2和v2组装成新的类作为k 2 ，
才能参与比较。所以在这里我们新建一个新的类型NewK2类型来封装原来的k2和v2。
 */