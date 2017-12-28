package com.hxf.hdfs;

/**
 * Created by fangqing on 12/28/17.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

/**
 * @Description: HDFS的操作类
 * @author: WH
 * @date: 2016-8-8 下午7:13:11
 */
public class HdfsServer {

    // 日志
    private static Logger log = Logger.getLogger(HdfsServer.class);
    private static Configuration conf = null;

    public String defaultAddress = "webhdfs://10.134.161.108:50070/";//设置hdfs的连接方式为webhdfs，通过HTTP访问hdfs


    private HdfsServer() {
        // TODO Auto-generated method stub
        conf = new Configuration();
        System.setProperty("java.security.krb5.conf", "/etc/krb5.conf");//设置kerberos配置信息
        conf.set("fs.defaultFS", defaultAddress);//namenode地址
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        conf.set("fs.webhdfs.impl", org.apache.hadoop.hdfs.web.WebHdfsFileSystem.class.getName());
        conf.setBoolean("hadoop.security.authentication", true);
        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("dfs.namenode.kerberos.principal", "hdfs/_HOST@TEST.COM");//hdfs-site.xml中配置信息
        conf.set("dfs.datanode.kerberos.principal", "hdfs/_HOST@TEST.COM");//hdfs-site.xml中配置信息
        UserGroupInformation.setConfiguration(conf);
        try {
            UserGroupInformation.loginUserFromKeytab("YJ100001", "/etc/hadoop.keytab");//kerberos 认证
            UserGroupInformation.getLoginUser();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    /**
     * @param path
     * @return
     */
    public boolean exits(String path) throws Exception {

        FileSystem fs = FileSystem.get(conf);
        return fs.exists(new Path(path));
    }


    public static void main(String[] args) {
        HdfsServer hd = new HdfsServer();
        try {
            System.out.println(hd.exits("/"));
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }


}


