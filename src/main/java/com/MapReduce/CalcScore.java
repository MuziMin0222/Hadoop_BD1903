package com.MapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
 * @package:com.MapReduce_Sort
 * @filename:CalcScore.java
 * @create:2019.10.08.14:12:01
 * @auther:李煌民
 * @description:.将学生的成绩集合到一个文件中
 **/
public class CalcScore extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new CalcScore(),args));
    }

    static class CalcScoreMapper extends Mapper<LongWritable, Text,Text,Text>{
        private Text k2 = new Text();
        private Text v2 = new Text();

        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
            String[] strs = v1.toString().split("[|]");

            this.k2.set(strs[1]);
            this.v2.set(strs[0] + "|" + strs[2]);

            context.write(this.k2,this.v2);
        }
    }

    static class CalcScoreReduce extends Reducer<Text,Text,Text,Text>{
        private Text k3 = new Text();
        private Text v3 = new Text();

        @Override
        protected void reduce(Text k2, Iterable<Text> v2s, Context context) throws IOException, InterruptedException {
            int count = 0;
            double sum = 0;

            StringBuffer sb = new StringBuffer();

            for (Text v2 : v2s) {
                String[] strs = v2.toString().split("[|]");
                switch (strs[0]){
                    case "a":{
                        sb.append("语文：");
                        break;
                    }
                    case "b":{
                        sb.append("英语：");
                        break;
                    }
                    case "c":{
                        sb.append("数学：");
                        break;
                    }
                }
                sb.append(strs[1]).append(",");
                sum += Double.parseDouble(strs[1]);
                count++;
            }
            sb.append("总分：").append(sum).append(",").append("平均值：").append(sum/count);

            this.k3.set(k2.toString());
            this.v3.set(sb.toString());
            context.write(this.k3,this.v3);
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();

        Path in = new Path(conf.get("in"));
        Path out = new Path(conf.get("out"));

        Job job = Job.getInstance(conf, "计算成绩，将数据写到一个文件中");
        job.setJarByClass(this.getClass());

        job.setMapperClass(CalcScoreMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,in);

        job.setReducerClass(CalcScoreReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,out);

        return job.waitForCompletion(true)?0:1;
    }
}
