package com.hxf.hdfs;

/**
 * Created by fangqing on 12/24/17.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;

public class CatHdfsFile {

    public static void readHdfs(String url) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(url), conf);
        InputStream in = null;
        try {
            in = fs.open(new Path(url));
            IOUtils.copyBytes(in, System.out, 4096, false);
        } finally {
            IOUtils.closeStream(in);
        }
    }



    public static void fileCopy(String localFile, String hdfsFile) throws IOException{
        InputStream in = new BufferedInputStream(new FileInputStream(localFile));
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(hdfsFile),conf);
        OutputStream out  = fs.create(new Path(hdfsFile),new Progressable(){
            public void progress(){
                System.out.print("*");
            }
        });
        IOUtils.copyBytes(in, out, 4096,true);
    }

    public static void readStatus(String url) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(url), conf);
        Path[] paths = new Path[1];
        paths[0] = new Path(url);
        FileStatus[] status = fs.listStatus(paths);
        Path[] listedPaths = FileUtil.stat2Paths(status);
        for (Path p : listedPaths) {
            System.out.println(p);
        }
    }



    public static void main(String[] args) throws IOException {
//        readHdfs("hdfs://localhost:9000/wordcountout/part-r-00000");
//        fileCopy("/home/fangqing/Downloads/atom.x86_64_001.rpm", "hdfs://localhost:9000/copys.iso");
        readStatus( "hdfs://localhost:9000/copys.iso");
    }
}