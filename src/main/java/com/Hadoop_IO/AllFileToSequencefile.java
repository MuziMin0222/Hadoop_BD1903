package com.Hadoop_IO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.junit.Test;

import java.io.*;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Set;

/**
 * @program:Hadoop_BD1903
 * @package:com.Hadoop_IO
 * @filename:AllFileToSequencefile.java
 * @create:2019.09.28.13:31:41
 * @auther:李煌民
 * @description:.将某一个文件夹下的所有文件都写到一个序列文件中
 **/
public class AllFileToSequencefile {

    @Test
    //将文件写到序列文件中，以绝对路径作为key，以文件内容作为value
    public void WriteFileToSeq() throws IOException {
        //获得集群对象
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://192.168.147.129:9000");

        //获得要写入到hdfs的文件对象
        Path path = new Path("/AllFile.seq");

        SequenceFile.Writer.Option o1 = SequenceFile.Writer.file(path);
        SequenceFile.Writer.Option o2 = SequenceFile.Writer.keyClass(Text.class);
        SequenceFile.Writer.Option o3 = SequenceFile.Writer.valueClass(MyFile.class);
        SequenceFile.Writer.Option o4 = SequenceFile.Writer.compression(SequenceFile.CompressionType.RECORD, new BZip2Codec());
        SequenceFile.Writer writer = SequenceFile.createWriter(conf, o1, o2, o3, o4);

        GetAllFile("D:\\code\\briup_code");

        Set<String> keySet = map.keySet();
        for (String key : keySet) {
            Text k = new Text(key);
            MyFile mf = map.get(key);
            writer.append(k,mf);
            writer.hflush();
        }
        writer.close();
    }

    //使用map集合来存储文件的绝对路径和内容
    private HashMap<String, MyFile> map = new HashMap<>();

    public void GetAllFile(String srcPath) throws IOException {
        //拿到一个文件夹的文件路径封装的对象
        File srcFolder = new File(srcPath);

        if (!srcFolder.exists()) return;

        //定义一个链表存储目录结构
        LinkedList<File> list = new LinkedList<>();

        //拿到该目录下的所有目录和文件
        File[] files = srcFolder.listFiles();
        ReadFileToMap(list, files);

        while (!list.isEmpty()){
            //拿到链表的第一个目录
            File dir_file = list.removeFirst();
            //拿到该目录下的所有文件
            files = dir_file.listFiles();
            ReadFileToMap(list, files);
        }
    }

    public void ReadFileToMap(LinkedList<File> list, File[] files) throws IOException {
        for (File file : files) {
            //是目录，就填加到链表中
            if (file.isDirectory()) {
                list.add(file);
            } else {
                //是文件，就将文件的绝对路径和文件内容添加到map集合中
                String path = file.getAbsolutePath();
                byte[] count = readFile(file);
                MyFile mf = new MyFile();
                mf.setCount(count);
                map.put(path, mf);
            }
        }
    }

    public byte[] readFile(File file) throws IOException {
        FileInputStream fis = new FileInputStream(file);
        byte[] bys = new byte[1024];
        byte[] count = new byte[0];
        int len;
        while ((len = fis.read(bys)) != -1){
            int length = count.length;
            byte[] temp = count;
            count = new byte[len + length];
            System.arraycopy(temp,0,count,0,temp.length);
            System.arraycopy(bys,0,count,length,len);
        }

        return count;
    }
}

//对读取的文件进行序列化
class MyFile implements Writable {
    private byte[] count;

    public MyFile(){
        count = new byte[0];
    }

    public byte[] getCount() {
        return count;
    }

    public void setCount(byte[] count) {
        this.count = count;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        new IntWritable(this.count.length).write(dataOutput);
        for (byte b : this.count) {
            new ByteWritable(b).write(dataOutput);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        IntWritable iw = new IntWritable();
        iw.readFields(dataInput);
        int size = iw.get();
        this.count = new byte[size];
        for (int i = 0; i < size; i++) {
            ByteWritable bw = new ByteWritable();
            bw.readFields(dataInput);
            this.count[i] = bw.get();
        }
    }
}