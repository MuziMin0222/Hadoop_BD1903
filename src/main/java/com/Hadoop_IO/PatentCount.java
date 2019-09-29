package com.Hadoop_IO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URI;
import java.util.HashMap;

/**
 * @program:Hadoop_BD1903
 * @package:com.Hadoop_IO
 * @filename:PatentCount.java
 * @create:2019.09.26.14:23:18
 * @auther:李煌民
 * @description:.专利统计：统计每一个专利被引用的次数
 **/
public class PatentCount {
    public static void main(String[] args) throws Exception{
        //获得集群信息
        Configuration con = new Configuration();
        con.set("fs.deaultFS","hdfs://192.168.147.129:9000");
        con.set("dfs.blocksize","32M");
        con.set("dfs.replication","1");

        //得到hdfs文件系统
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.147.129:9000"), con, "lhm");

        //获得文件对象
        Path path = new Path("/data/patent/cite75_99.txt");

        //访问该文件
        FSDataInputStream fsdis = fs.open(path);
        BufferedReader br = new BufferedReader(new InputStreamReader(fsdis));

        //定义一个map集合来存储处理后的数据
        HashMap<String, Integer> map = new HashMap<>();

        String line;
        while ((line = br.readLine()) != null){
            String[] strs = line.split("[,]");
            String value = strs[1];
            map.put(value,map.containsKey(value)?map.get(value)+1:1);
        }

        //将文件写入到hdfs集群中
        FSDataOutputStream fsdos = fs.create(new Path("/pcount.txt"));
        PrintWriter pw = new PrintWriter(fsdos,true);
        map.forEach((k,v)->{
            pw.println(k + "\t" + v);
        });
    }
}
