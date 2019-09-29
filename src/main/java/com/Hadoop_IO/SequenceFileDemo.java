package com.Hadoop_IO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.net.URI;
import java.util.Random;

/**
 * @program:Hadoop_BD1903
 * @package:com.Hadoop_IO
 * @filename:SequenceFileDemo.java
 * @create:2019.09.26.08:38:28
 * @auther:李煌民
 * @description:.测试序列化文件的读写操作
 **/
public class SequenceFileDemo {
    @Test
    //序列化文件的读操作
    public void SequenceRead() throws Exception{
        //获取集群对象
        Configuration con = new Configuration();
        con.set("fs.defaultFS","hdfs://192.168.147.129:9000");

        //获得hdfs文件系统对象
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.147.129:9000"), con, "lhm");

        //获取文件对象
        Path path = new Path("/number.seq");

        //获取序列化文件读取对象
        SequenceFile.Reader reader = new SequenceFile.Reader(con, SequenceFile.Reader.file(path));

        IntWritable key = new IntWritable();
        Text value = new Text();

        long position = reader.getPosition();

        //迭代获取数据
        while (reader.next(key,value)){
            //判断是否看到了同步标记
            boolean flag = reader.syncSeen();
            String sync;
            if (flag){
                sync = "**";
            }else{
                sync = "  ";
            }

            System.out.println(position + "---" + sync + "---" + key + "---" + value);
        }

        reader.close();
    }

    @Test
    //序列化文件的写操作
    public void SequenceWrite() throws Exception {
        //获取集群信息
        Configuration con = new Configuration();
        con.set("fs.defaultFS","hdfs://192.168.147.129:9000");

        //获取文件系统对象
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.147.129:9000"), con, "lhm");

        //获取文件对象
        Path path = new Path("/number.seq");

        IntWritable key = new IntWritable();
        Text value = new Text();

        //获取SequenceFile的写入对象
        SequenceFile.Writer writer = SequenceFile.createWriter(con,
                SequenceFile.Writer.file(path),
                SequenceFile.Writer.keyClass(key.getClass()),
                SequenceFile.Writer.valueClass(value.getClass()));

        //数据
        String[] DATA={"One, two, buckle my shoe","Three, four, shut the door","Five, six, pick up sticks",
                "Seven, eight, lay them straight","Nine, ten, a big fat hen"};

        for (int i = 0; i < 100; i++) {
            key.set(100-i);
            Random random = new Random();
            int num = random.nextInt(DATA.length);
            value.set(DATA[num]);

            //写入数据
            writer.append(key,value);
        }

        writer.close();
    }
}
