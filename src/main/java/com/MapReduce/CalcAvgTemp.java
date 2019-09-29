package com.MapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @program:Hadoop_BD1903
 * @package:com.MapReduce
 * @filename:CalcAvgTemp.java
 * @create:2019.09.27.17:03:03
 * @auther:李煌民
 * @description:.计算1992年每个气象站的平均气温
 *
 * 即每个气象站的编号为k3，每个气象站的平均气温为v3
 **/
public class CalcAvgTemp extends Configured implements Tool {

    //1、map任务
    static class CalcAvgTempMapper extends Mapper<LongWritable, Text,Text, DoubleWritable> {
        private Text k2 = new Text();
        private DoubleWritable v2 = new DoubleWritable();

        //获得解析原始数据类
        private  RawWeatherDataParser parser = new RawWeatherDataParser();

        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
            //拿到每一行数据v1，通过原始天气数据解析来得到k2和v2
            //用来判断该数据是否正确
            boolean flag = this.parser.parse(v1);

            //获得数据
            if (flag){
                //得到气象台的编号
                this.k2.set(this.parser.getSid());
                //得到温度数据
                this.v2.set(this.parser.getTemp());

                //将map处理的数据交给reduce端
                context.write(k2,v2);
            }
        }
    }

    //2、reduce端
    static class CalcAvgTempReduce extends Reducer<Text, DoubleWritable,Text,DoubleWritable> {
        private Text k3 = new Text();
        private DoubleWritable v3 = new DoubleWritable();

        @Override
        protected void reduce(Text k2, Iterable<DoubleWritable> v2s, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;

            //对map传过来的value值即温度值进行
            for (DoubleWritable v2 : v2s) {
                sum += v2.get();
                count++;
            }

            double avg = sum/count;

            this.k3.set(k2);
            this.v3.set(avg);
            context.write(k3,v3);
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        //拿到conf对象
        Configuration conf = this.getConf();

        //配置文件输入路径和文件输出路径
        Path in = new Path(conf.get("in"));
        Path out = new Path(conf.get("out"));

        //获得job对象
        Job job = Job.getInstance(conf);
        job.setJarByClass(this.getClass());

        //配置map端任务
        job.setMapperClass(CalcAvgTempMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,in);

        //配置reduce端任务
        job.setReducerClass(CalcAvgTempReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,out);

        //设置reduce的个数
        job.setNumReduceTasks(5);

        //设置Combiner类
//        job.setCombinerClass();
        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new CalcAvgTemp(),args));
    }
}
