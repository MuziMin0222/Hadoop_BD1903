package com.MapReduce_Join.reduce_join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @program:Hadoop_BD1903
 * @package:com.MapReduce_Join
 * @filename:ReduceJoin.java
 * @create:2019.10.09.16:41:28
 * @auther:李煌民
 * @description:.Reduce端的Join连接操作
 **/
public class ReduceJoin extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new ReduceJoin(),args));
    }

    static class JoinMapper1 extends Mapper<Text,Text,KeyFlag,Text>{
        private KeyFlag k2 = new KeyFlag();
        private Text v2 = new Text();

        @Override
        protected void map(Text k1, Text v1, Context context) throws IOException, InterruptedException {
            this.k2.setKey(k1);
            this.k2.setFlag("a");
            this.v2.set(v1.toString());
            context.write(k2,v2);
        }
    }

    static class JoinMapper2 extends Mapper<Text,Text,KeyFlag,Text>{
        private KeyFlag k2 = new KeyFlag();
        private Text v2 = new Text();

        @Override
        protected void map(Text k1, Text v1, Context context) throws IOException, InterruptedException {
            this.k2.setKey(k1);
            this.k2.setFlag("b");
            this.v2.set(v1.toString());
            context.write(k2,v2);
        }
    }

    static class JoinReduce extends Reducer<KeyFlag,Text, NullWritable,Text>{
        private NullWritable k3 = NullWritable.get();
        private Text v3 = new Text();

        @Override
        protected void reduce(KeyFlag k2, Iterable<Text> v2s, Context context) throws IOException, InterruptedException {
            StringBuffer sb = new StringBuffer();
            sb.append(k2.getKey().toString()).append(",");
            v2s.forEach(v2->{
                sb.append(v2.toString()).append(",");
            });
            this.v3.set(sb.substring(0,sb.length()-1));
            context.write(this.k3,this.v3);
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();

        Path in1 = new Path(conf.get("in1"));
        Path in2 = new Path(conf.get("in2"));
        Path out = new Path(conf.get("out"));

        //自定义分割器
//        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR,",");

        Job job = Job.getInstance(conf, "Reducejoin连接操作");
        job.setJarByClass(this.getClass());

        //多输入技术
        MultipleInputs.addInputPath(job,in1, KeyValueTextInputFormat.class,JoinMapper1.class);
        MultipleInputs.addInputPath(job,in2,KeyValueTextInputFormat.class,JoinMapper2.class);
        job.setMapOutputKeyClass(KeyFlag.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(JoinReduce.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,out);

        //设置自定义分区器
        job.setPartitionerClass(MyPartitioner.class);

        //设置自定义分组比较器
        job.setGroupingComparatorClass(MyComparator.class);

        return job.waitForCompletion(true)?0:1;
    }
}
