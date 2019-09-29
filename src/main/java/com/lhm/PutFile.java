package com.lhm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


/**
 * @program:Hadoop_BD1903
 * @package:com.lhm
 * @filename:PutFile.java
 * @create:2019.09.24.16:55:24
 * @auther:李煌民
 * @description:.上传文件到HDFS集群
 **/
public class PutFile {
    @Test
    public void test() throws IOException, URISyntaxException, InterruptedException {
        //获取configuration对象，配置集群信息
        Configuration con = new Configuration();
        //c:\windows\System32\drivers\etc\hosts配置ip映射,如果在Windows上运行该步骤必不可少
//        con.set("fs.defaultFS","hdfs://192.168.147.129:9000");
        //获取文件系统对象
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.147.129:9000"),con,"lhm");
        //创建文件
        Path path = new Path("/lhm.txt");
        FSDataOutputStream fsdos = fs.create(path);

        FileInputStream fis = new FileInputStream("D:\\code\\IODemo\\rmptmp.txt");

        //边读边写
        IOUtils.copyBytes(fis,fsdos,1024,true);

        //若出现winutils.exe问题
        // 1.在windows中安装hadoop并配置环境变量
        // 2.将文件winutils.exe拷贝至hadoop安装目录的bin目录下
    }
}
