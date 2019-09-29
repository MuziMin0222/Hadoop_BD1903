package com.Hadoop_IO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.*;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URI;

/**
 * @program:Hadoop_BD1903
 * @package:com.Hadoop_IO
 * @filename:CompressionTest.java
 * @create:2019.09.25.14:30:10
 * @auther:李煌民
 * @description:.测试压缩和解压缩
 **/
public class CompressionTest{
    public static void main(String[] args) throws Exception {
        //testCompression();
        testDeCompression();
    }

    //压缩文件上传到集群中
    public static void testCompression() throws Exception{
        Configuration con = new Configuration();
        con.set("fs.defaultFS","hdfs://192.168.147.129:9000");
        //数据块的大小是由客户端指定的
        //以下配置是在客户端生效
        con.set("dfs.blocksize","32M");
        con.set("dfs.replication","1");

        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.147.129:9000"), con, "lhm");

        /*
        上传文件需要压缩，需要编码器
        1、compressionCodecFactory通过工厂模式会根据上传集群文件的扩展名自动获取对应的编码器
        2、在代码中直接通过new创建相应的编码器对象
         */
        String filePath = "/a.deflate";

        //正常的输出流对象
        FSDataOutputStream fsdos = fs.create(new Path(filePath));

        //方法1：要对数据进行压缩，则需要使用编码器对输出流进行压缩
        CompressionCodecFactory factory = new CompressionCodecFactory(con);
        CompressionCodec codec = factory.getCodec(new Path(filePath));
        CompressionOutputStream cos = codec.createOutputStream(fsdos);

//        //方法2：指定gzip编码器来进行压缩数据
//        GzipCodec codec1 = new GzipCodec();
//        codec1.setConf(con);
//        CompressionOutputStream cos1 = codec1.createOutputStream(fsdos);
//
//        //方法3：使用deflate编码器压缩数据
//        DeflateCodec codec2 = new DeflateCodec();
//        codec2.setConf(con);
//        CompressionOutputStream cos2 = codec2.createOutputStream(fsdos);
//
//        //方法4：使用bzip2编码器来压缩数据
//        BZip2Codec codec3 = new BZip2Codec();
//        codec3.setConf(con);
//        CompressionOutputStream cos3 = codec3.createOutputStream(fsdos);

        //从本地中获取文件对象
        FileInputStream fis = new FileInputStream("D:\\code\\IODemo\\rmptmp.txt");

        //边读边写
        IOUtils.copyBytes(fis,cos,1024,true);
    }


    //测试解压缩
    public static void testDeCompression() throws Exception{
        Configuration con = new Configuration();
        con.set("fs.defaultFS","hdfs://192.168.147.129:9000");

        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.147.129:9000"), con, "lhm");

        Path path = new Path("/a.deflate");

        FSDataInputStream fsdis = fs.open(path);

        //法1,根据文件后缀名自动匹配对应的解码器
        CompressionCodecFactory factory = new CompressionCodecFactory(con);
        CompressionCodec codec = factory.getCodec(path);
        CompressionInputStream cis = codec.createInputStream(fsdis);

        //法2,使用gzip解码器来解压缩文件
        GzipCodec codec1 = new GzipCodec();
        codec1.setConf(con);
        CompressionInputStream cis1 = codec1.createInputStream(fsdis);

        //法3：使用deflate解码器来解压缩文件
        DeflateCodec codec2 = new DeflateCodec();
        codec2.setConf(con);
        CompressionInputStream cis2 = codec2.createInputStream(fsdis);

        //法4：使用bzip2解码器来解压缩文件
        BZip2Codec codec3 = new BZip2Codec();
        codec3.setConf(con);
        CompressionInputStream cis3 = codec3.createInputStream(fsdis);

        BufferedInputStream bis = new BufferedInputStream(cis);
        byte[] bys = new byte[1024];
        int len;
        while ((len = bis.read(bys)) != -1){
            System.out.println(new String(bys,0,len));
        }
    }
}
