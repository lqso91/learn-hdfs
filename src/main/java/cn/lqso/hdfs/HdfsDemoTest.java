package cn.lqso.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.*;

/**
 * @author luojie
 */
public class HdfsDemoTest {

    private String host = "172.17.0.2";

    @Test
    public void download() {
        //指定当前的Hadoop的用户
        System.setProperty("HADOOP_USER_NAME", "root");

        //配置参数：指定NameNode地址
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", String.format("hdfs://%s:9000", host));

        DistributedFileSystem fileSystem = null;
        InputStream input = null;

        try {
            //创建一个客户端
            fileSystem = (DistributedFileSystem) FileSystem.get(conf);

            //构造一个输入流，从HDFS中读取数据
            input = fileSystem.open(new Path("/exe/ideaIU-2018.3.exe"));

            //构造一个输出流，输出到本地的目录
            OutputStream output = new FileOutputStream("D:\\tmp\\ideaIU-2018.3.exe");

            //使用工具类
            IOUtils.copyBytes(input, output, 1024, true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getNodeInfo() {
        //指定当前的Hadoop的用户
        System.setProperty("HADOOP_USER_NAME", "root");

        //配置参数：指定NameNode地址
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", String.format("hdfs://%s:9000", host));

        try {
            //创建一个客户端
            DistributedFileSystem fileSystem = (DistributedFileSystem) FileSystem.get(conf);
            DatanodeInfo[] dataNodeStats = fileSystem.getDataNodeStats();

            for (DatanodeInfo info : dataNodeStats) {
                System.out.println(info);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void upload() {
        //指定当前的Hadoop的用户
        System.setProperty("HADOOP_USER_NAME", "root");

        //配置参数：指定NameNode地址
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", String.format("hdfs://%s:9000", host));
        DistributedFileSystem fileSystem = null;
        InputStream input = null;

        try {
            //创建一个客户端
            fileSystem = (DistributedFileSystem) FileSystem.get(conf);
            FSDataOutputStream output = fileSystem.create(new Path("/tmp/a.txt"), true);
            String s = "hello a.txt \n hello 123";
            input = new ByteArrayInputStream(s.getBytes());
            IOUtils.copyBytes(input, output, 1024, true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
