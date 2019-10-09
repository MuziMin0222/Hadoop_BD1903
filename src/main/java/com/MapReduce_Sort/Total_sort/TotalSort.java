package com.MapReduce_Sort.Total_sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URI;

/**
 * @program:Hadoop_BD1903
 * @package:com.MapReduce_Sort
 * @filename:TotalSort.java
 * @create:2019.10.08.09:04:24
 * @auther:李煌民
 * @description:.全局排序,找到每一年中的最高温度
 * 存在问题：如果Map任务k1的数据类型和k2的数据类型不一致时，会报异常；
 * 当k1和k2的数据类型一致时，且都为LongWritable，
 * 不能对文本文件中的数据进行排序；其实也能，但是所有的数据只会进入同一个Reduce；
 **/
public class TotalSort extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new TotalSort(),args));
    }

    //设置map端任务
    static class TotalSortMapper extends Mapper<LongWritable, Text,LongWritable,DoubleWritable>{
        private LongWritable k2 = new LongWritable();
        private DoubleWritable v2 = new DoubleWritable();

        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
            String[] strs = v1.toString().split("[\t]");
            this.k2.set(Long.parseLong(strs[0]));
            this.v2.set(Double.parseDouble(strs[2]));

            context.write(k2,v2);
        }
    }

    //设置reduce端任务
    static class TotalSortReduce extends Reducer<LongWritable,DoubleWritable,LongWritable,DoubleWritable>{
        private LongWritable k3 = new LongWritable();
        private DoubleWritable v3 = new DoubleWritable();

        @Override
        protected void reduce(LongWritable k2, Iterable<DoubleWritable> v2s, Context context) throws IOException, InterruptedException {
            double max = -Double.MAX_VALUE;
            for (DoubleWritable v2 : v2s) {
                if (v2.get() > max){
                    max = v2.get();
                }
            }
            k3.set(k2.get());
            v3.set(max);
            context.write(k3,v3);
        }
    }

    //配置作业任务
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();

        //配置文件输入输出路径
        Path in = new Path(conf.get("in"));
        Path out = new Path(conf.get("out"));

        Job job = Job.getInstance(conf, "全局排序");
        job.setJarByClass(this.getClass());

        job.setMapperClass(TotalSortMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,in);

        job.setReducerClass(TotalSortReduce.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,out);

        //设置reduce的个数
        job.setNumReduceTasks(3);

        //每一个reducer的输出在默认的情况下都是有顺序的，但是reducer之间在输入是无序的情况下也是无序的。
        // 如果要实现输出是全排序的那就会用到TotalOrderPartitioner
        job.setPartitionerClass(TotalOrderPartitioner.class);

        InputSampler.RandomSampler sampler = new InputSampler.RandomSampler(0.8, 10000, 3);

        InputSampler.writePartitionFile(job,sampler);

        String partitionFile = TotalOrderPartitioner.getPartitionFile(conf);

        job.addCacheFile(new URI(partitionFile));
        return job.waitForCompletion(true)?0:1;
    }
}