package com.MapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @program:Hadoop_BD1903
 * @package:com.MapReduce
 * @filename:WeatherDataToFile.java
 * @create:2019.09.29.11:10:39
 * @auther:李煌民
 * @description:.year sid  temp形式形成文本文件，生成三列数据文件
 **/
public class WeatherDataToFile extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new WeatherDataToFile(),args));
    }

    //1、map端
    static class WeatherDataToFileMapper extends Mapper<LongWritable, Text,IntWritable,Text>{
        private IntWritable k2 = new IntWritable();
        private Text v2 = new Text();
        private RawWeatherDataParser parser = new RawWeatherDataParser();

        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
            boolean flag = parser.parse(v1);

            if (flag){
                int year = this.parser.getYear();
                String sid = this.parser.getSid();
                double temp = this.parser.getTemp();
                String sid_temp = sid + "\t" + temp;

                k2.set(year);
                v2.set(sid_temp);
                context.write(k2,v2);
            }
        }
    }

    //因为reduce端不用处理数据，所以可以不用写

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();

        Path in = new Path(conf.get("in"));
        Path out = new Path(conf.get("out"));

        String outputType = conf.get("output.type");

        Job job = Job.getInstance(conf,"生成三列数据集");
        job.setJarByClass(this.getClass());

        job.setMapperClass(WeatherDataToFileMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job,in);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        if ("seq".equals(outputType)){
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
        }else {
            job.setOutputFormatClass(TextOutputFormat.class);
        }
        FileOutputFormat.setOutputPath(job,out);

        //Partitioner 组件可以对 Map处理后的键值对 按Key值 进行分区，从而将不同分区的 Key 交由不同的 Reduce 处理。
        job.setPartitionerClass(HashPartitioner.class);

        return job.waitForCompletion(true)?0:1;
    }
}
