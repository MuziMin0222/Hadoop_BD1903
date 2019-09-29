package com.lhm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;

/**
 * @program:Hadoop_BD1903
 * @package:com.lhm
 * @filename:GetFile.java
 * @create:2019.09.24.23:00:13
 * @auther:李煌民
 * @description:.测试从hdfs文件系统中下载文件到本地中
 **/
public class GetFile {
    @Test
    public void test() throws Exception {
        //获取集群
        Configuration con = new Configuration();

        //获取该集群的文件系统对象
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.147.129:9000"), con, "lhm");

        //从该文件系统获得文件路径
        Path path = new Path("/lhm.txt");

        //FileStatus相当于java中的File
        FileStatus[] fileStatuses = fs.listStatus(path);
        for (FileStatus fileStatus : fileStatuses) {
            System.out.println(fileStatus);
        }

        //j将该文件转为输入流
        FSDataInputStream fsdis = fs.open(path);
        BufferedReader br = new BufferedReader(new InputStreamReader(fsdis));

        String line;
        while ((line=br.readLine())!= null){
            System.out.println(line);
        }

        br.close();
    }
}
