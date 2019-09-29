package com.lhm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.net.URI;

/**
 * @program:Hadoop_BD1903
 * @package:com.lhm
 * @filename:MoveFile.java
 * @create:2019.09.25.10:43:30
 * @auther:李煌民
 * @description:.在hdfs中进行文件的移动
 **/
public class MoveFile {
    @Test
    public void test() throws Exception{
        Configuration con = new Configuration();
        con.set("fs.defaultFS","hdfs://192.168.147.129:9000");

        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.147.129:9000"), con, "lhm");

        Path srcfile = new Path("/copy.txt");
        Path destfile = new Path("/mv.txt");

        FSDataInputStream fsdis = fs.open(srcfile);
        FSDataOutputStream fsdos = fs.create(destfile);

        IOUtils.copyBytes(fsdis,fsdos,1024,true);

       fs.delete(srcfile,true);
    }
}
