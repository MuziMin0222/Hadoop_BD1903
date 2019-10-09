package com.MapReduce_Join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @program:Hadoop_BD1903
 * @package:com.MapReduce_Join
 * @filename:HandlePre.java
 * @create:2019.10.08.14:50:39
 * @auther:李煌民
 * @description:.预处理数据
 **/
public class HandlePre extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new HandlePre(),args));
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();

        Path in = new Path(conf.get("in"));
        Path out = new Path(conf.get("out"));

        //以分割符将数据分为key和value来将数据读取到map端中
        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR,",");

        Job job = Job.getInstance(conf, "对数据进行预处理");
        job.setJarByClass(this.getClass());

        //使用默认的map程序
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job,in);

        //使用默认的reduce程序
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job,out);
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

        job.setNumReduceTasks(1);

        return job.waitForCompletion(true)?0:1;
    }
}
