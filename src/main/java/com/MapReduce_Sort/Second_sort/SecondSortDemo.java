package com.MapReduce_Sort.Second_sort;

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
 * @package:com.MapReduce_Sort
 * @filename:MaxTempEachYearSid.java
 * @create:2019.09.30.09:46:11
 * @auther:李煌民
 * @description:.二次排序的测试
 **/
public class SecondSortDemo extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new SecondSortDemo(),args));
    }

    static class SecondSortMapper extends Mapper<LongWritable, Text, YearSid, DoubleWritable>{
        private YearSid k2 = new YearSid();
        private DoubleWritable v2 = new DoubleWritable();

        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
            String[] strs = v1.toString().split("[\t]");
            this.k2.setYear(Integer.parseInt(strs[0]));
            this.k2.setSid(strs[1]);
            this.v2.set(Double.parseDouble(strs[2]));
            context.write(this.k2,this.v2);
        }
    }

    static class SecondSortReduce extends Reducer<YearSid,DoubleWritable, YearSid,DoubleWritable>{
        private YearSid k3 = new YearSid();
        private DoubleWritable v3 = new DoubleWritable();

        @Override
        protected void reduce(YearSid k2, Iterable<DoubleWritable> v2s, Context context) throws IOException, InterruptedException {
            for (DoubleWritable v2 : v2s) {
                this.k3.setYear(k2.getYear());
                this.k3.setSid(k2.getSid());
                this.v3.set(v2.get());
                context.write(this.k3,this.v3);
            }
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();

        Job job = Job.getInstance(conf,"二次排序的测试");
        job.setJarByClass(SecondSortDemo.class);

        Path in = new Path(conf.get("in"));
        Path out = new Path(conf.get("out"));

        job.setMapperClass(SecondSortMapper.class);
        job.setMapOutputKeyClass(YearSid.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,in);

        job.setReducerClass(SecondSortReduce.class);
        job.setOutputKeyClass(YearSid.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,out);

        //设置reduce的个数
        job.setNumReduceTasks(3);

        //设置分区器
        job.setPartitionerClass(MyPartitioner.class);

        //设置分组比较器
        job.setGroupingComparatorClass(MyComparator.class);

        return job.waitForCompletion(true)?0:1;
    }
}
