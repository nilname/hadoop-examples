package com.hxf.hdfs.utils;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


/**
 * Created by fangqing on 12/25/17.
 */
public class MySortTool {

    public static class MyMapper extends
            Mapper<Object, Text, IntWritable, IntWritable> {
        //Map阶段的两个压入context的参数（也就是第3、4个参数）类型皆修改为IntWritable，而不是Text
        public void map(Object key, Text value, org.apache.hadoop.mapreduce.Mapper.Context context)
                throws IOException, InterruptedException {
            IntWritable data = new IntWritable(Integer.parseInt(value.toString()));//将输入文件的每一行擦写成IntWritable
            IntWritable random = new IntWritable(new Random().nextInt());//搞个随机数
            context.write(data, random);
        }
    }

    public static class MyReducer extends
            Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        //将reducer得到的两个数据类型（第1、2个参数）标识为IntWritable，而不是Text
        //将reducer写到文件的两个数据类型（第3、4个参数）标识为IntWritable，而不是Text
        public void reduce(IntWritable key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            while (values.iterator().hasNext()) {//遍历values，有1个随机数，输出一次key
                context.write(key, null);
                values.iterator().next();//记得遍历的时候，将游标（迭代器）向后推
            }
        }
    }



    public static class IntWritableDecreasingComparator extends
            IntWritable.Comparator {
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "");
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(IntWritable.class);//表示写到文件的Key是IntWritable，而不是Text
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
//        job.setSortComparatorClass(IntWritableDecreasingComparator.class);//设置Sort阶段使用比较器
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

/*
Mapreduce在Map与Reduce之间的处理，会对Key进行升序排序，
如果这个Key是Text类型则是按Key的首字母进行升序排序的，
如果Key是IntWritable类型，则按大小进行升序排序，利用这点，可以对数据进行排序。

比如如下的数据：
1
4
7
2
5
9
3
6
8
要排成如下的形式：
1
2
3
4
5
6
7
8
9
在Map过程将这些数据摆进Context这个数据字典的时候，除了需要注意类型的匹配以外，还要注意Mapreduce对Key的去重特性
。因此，在Map的Key是这个值，Value应该放一个随机数去占Reduce阶段中Iterable<IntWritable> values的位置，
到Reduce输出到文件阶段，Iterable<IntWritable> values有多少个随机数，
就向文件输出多少个Iterable<IntWritable> values所对应的Key



那么，如果我想降序排序以上的结果需要自己重写Hadoop的的比较器IntWritable.Comparator如IntWritableDecreasingComparator，重写原来的返回值，将其正数改成负数，
同时在主函数设置Mapreduce在Map与Reduce中间的Sort阶段使用这个比较器 job.setSortComparatorClass(IntWritableDecreasingComparator.class);

 */