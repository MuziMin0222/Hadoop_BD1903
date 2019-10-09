package com.MapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.map.TokenCounterMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @program:Hadoop_BD1903
 * @package:com.MapReduce
 * @filename:ChainMapperAndReducer.java
 * @create:2019.09.29.22:56:45
 * @auther:李煌民
 * @description:.Mapper和reduce的链式调用
 **/
public class ChainMapperAndReducer extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new ChainMapperAndReducer(),args));
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        //KeyValueTextInputFormat，自定义格式来分割读取一行中的数据，默认是以\t分割
        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR,",");

        //设置文件输入输出路径
        Path in = new Path(conf.get("in"));
        Path out = new Path(conf.get("out"));

        Job job = Job.getInstance(conf, "MapReduce链式调用");
        job.setJarByClass(this.getClass());

        //反转key值和value值
        ChainMapper.addMapper(
                job, InverseMapper.class,
                Text.class,Text.class,//输入map端的数据类型
                Text.class,Text.class,//输出map端的数据类型
                conf
        );

        //将输入分解为独立的单词，输出个单词和计数器
        ChainMapper.addMapper(
                job, TokenCounterMapper.class,
                Text.class,Text.class,
                Text.class, IntWritable.class,
                conf
        );

        //将map出来的数据交给reduce处理,对各key的所有整型值求和
        ChainReducer.setReducer(
                job, IntSumReducer.class,
                Text.class,IntWritable.class,
                Text.class,IntWritable.class,
                conf
        );

        //在reduce端继续使用map将key和value互换
        ChainReducer.addMapper(
                job,InverseMapper.class,
                Text.class,IntWritable.class,
                IntWritable.class,Text.class,
                conf
        );

        //如果行中有分隔符，那么分隔符前面的作为key，后面的作为value；如果没有分隔符，那么整行作为key，value为空
        //当输入数据的每一行是两列，并用\t分离的形式的时候，KeyValueTextInputformat处理这种格式的文件非常适合。也可以自定义分隔符
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job,in);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job,out);

        return job.waitForCompletion(true)?0:1;
    }
}
