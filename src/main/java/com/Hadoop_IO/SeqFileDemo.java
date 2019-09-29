package com.Hadoop_IO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @program:Hadoop_BD1903
 * @package:com.Hadoop_IO
 * @filename:SeqFileDemo.java
 * @create:2019.09.26.20:00:00
 * @auther:李煌民
 * @description:.生成序列文件，存储10000个小文件； Key是文件的文件绝对路径，Value是文件的内容；
 **/
public class SeqFileDemo {

    @Test
    //将小文件从hdfs中读出来
    public void SeqFileReader() throws Exception{
        //获得集群对象
        Configuration con = new Configuration();
        con.set("fs.defaultFS","hdfs://192.168.147.129:9000");

        //获取文件路径
        Path path = new Path("/se.seq");

        //读序列文件时，使用option对象来表示文件路径
        SequenceFile.Reader.Option o1 = SequenceFile.Reader.file(path);
        SequenceFile.Reader reader = new SequenceFile.Reader(con, o1);

        //key和value都是Text类型
        Text key = new Text();
        Text value = new Text();

        //判断该序列文件中有没有下一个能够读取的Record
        while (reader.next(key,value)){
            String line;
            if (reader.syncSeen()){
                //如果看到了同步标记
                line = "sync" + key + "===" + value;
            }else {
                line = "    " + key + "===" + value;
            }
            System.out.println(line);
        }
    }

    //将小文件写到集群中
    @Test
    public void SeqFileWriter() throws Exception {
        //获得集群对象
        Configuration con = new Configuration();
        con.set("fs.defaultFS","hdfs://192.168.147.129:9000");

        Path path = new Path("/se.seq");

        //在生成序列文件时，必须使用Option对象来表示生成的文件路径
        SequenceFile.Writer.Option o1 = SequenceFile.Writer.file(path);
        // Key的数据类型
        SequenceFile.Writer.Option o2 = SequenceFile.Writer.keyClass(Text.class);
        //value的数据类型
        SequenceFile.Writer.Option o3 = SequenceFile.Writer.valueClass(Text.class);
        //是否进行压缩，选择其压缩格式
        SequenceFile.Writer.Option o4 = SequenceFile.Writer.compression(SequenceFile.CompressionType.RECORD, new BZip2Codec());
        //参数是Configuration对象和Option对象的可变参数
        SequenceFile.Writer writer = SequenceFile.createWriter(con,o1,o2,o3,o4);

        //得到小文件存放目录
        File dir = new File("D:\\code\\IODemo\\SmallFile");
        //得到该目录下的所有小文件
        File[] files = dir.listFiles();

        //判断该目录下是否为空
        if (files == null)return;

        int count = 0;
        for (File file : files) {
            //拿到每一个小文件的绝对路径，并作为key值
            Text key = new Text(file.getAbsolutePath());

            //读取文件内容作为value值
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
            String line;
            StringBuffer sb = new StringBuffer();
            while ((line = br.readLine()) != null){
                sb.append(line).append(System.lineSeparator());
            }
            Text value = new Text(sb.toString());

            //每隔十个文件就添加一个同步标记
            if (count++ % 10 == 0){
                writer.sync();
            }

            writer.append(key,value);
            writer.hflush();
            br.close();
        }
        writer.close();
    }
}
