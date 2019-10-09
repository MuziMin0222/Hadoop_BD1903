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
 * @filename:CalcAvgTempUseCombinner.java
 * @create:2019.09.29.18:55:57
 * @auther:李煌民
 * @description:.使用Combiner来优化计算每个气象站的平均温度
 **/
public class CalcAvgTempUseCombinner extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new CalcAvgTempUseCombinner(),args));
    }

    //map端任务
    static class CalcMapper extends Mapper<LongWritable, Text,Text,AvgNum>{
        private Text k2 = new Text();
        private AvgNum v2 = new AvgNum();
        private RawWeatherDataParser parser = new RawWeatherDataParser();

        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
            if (parser.parse(v1)){
                this.k2.set(parser.getSid());
                this.v2.setAvg(parser.getTemp());
                //因为读一行数据只有一个温度值，map处理之后，同一个气象站的温度值构成一个集合
                this.v2.setNum(1L);

                context.write(k2,v2);
            }
        }
    }

    //Combiner端
    static class CalcCombinner extends Reducer<Text,AvgNum,Text,AvgNum>{
        //只对value值进行处理，所以不用定义key值
        private AvgNum v2c = new AvgNum();

        @Override
        protected void reduce(Text k2c, Iterable<AvgNum> v2cs, Context context) throws IOException, InterruptedException {
            //1、计算一共有多少个数据
            long count = 0;
            double sum = 0;
            //2、计算该map数据片发送过来的同一个气象站的所有的数据产生的平均值
            for (AvgNum v : v2cs) {
                sum += v.getAvg().get()*v.getNum().get();
                count += v.getNum().get();
            }
            double avg = sum/count;
            v2c.setAvg(avg);
            v2c.setNum(count);

            context.write(k2c,v2c);
        }
    }

    //reduce端
    static class CalcReduce extends Reducer<Text,AvgNum,Text, DoubleWritable>{
        private DoubleWritable v3 = new DoubleWritable();

        @Override
        protected void reduce(Text k2c, Iterable<AvgNum> v2cs, Context context) throws IOException, InterruptedException {
            //1、计算一共有多少个数据
            long count = 0;
            double sum = 0;
            //2、计算该map数据片发送过来的同一个气象站的所有的数据产生的平均值
            for (AvgNum v : v2cs) {
                count += v.getNum().get();
                sum += v.getAvg().get()*v.getNum().get();
            }
            double avg = sum/count;
            v3.set(avg);

            context.write(k2c,v3);
        }
    }

    //作业配置
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();

        Path in = new Path(conf.get("in"));
        Path out = new Path(conf.get("out"));

        Job job = Job.getInstance(conf,"使用Combiner来优化计算每个气象站的平均值");
        job.setJarByClass(this.getClass());

        //配置map端任务
        job.setMapperClass(CalcMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(AvgNum.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,in);

        //设置Combiner
        job.setCombinerClass(CalcCombinner.class);

        //配置reduce端任务
        job.setReducerClass(CalcReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,out);

        return job.waitForCompletion(true)?0:1;
    }
}
