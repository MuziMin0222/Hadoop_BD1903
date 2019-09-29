package com.lhm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;

/**
 * @program:Hadoop_BD1903
 * @package:com.lhm
 * @filename:CopyFile.java
 * @create:2019.09.25.10:30:29
 * @auther:李煌民
 * @description:.在hdfs中进行文件的复制
 **/
public class CopyFile {
    @Test
    public void test() throws Exception{
        Configuration con = new Configuration();
        con.set("fs.deaultFS","hdfs://192.168.147.129:9000");

        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.147.129:9000"), con, "lhm");
        Path srcFile = new Path("/lhm.txt");
        Path destFile = new Path("/copy.txt");

        FSDataInputStream fsdis = fs.open(srcFile);
        FSDataOutputStream fsdos = fs.create(destFile);

        IOUtils.copyBytes(fsdis,fsdos,1024,true);
    }
}
