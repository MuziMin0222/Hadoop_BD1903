package com.MapReduce_Join.map_join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputFormat;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;

/**
 * @program:Hadoop_BD1903
 * @package:com.MapReduce_Join
 * @filename:MapJoin.java
 * @create:2019.10.09.10:14:50
 * @auther:李煌民
 * @description:.map端数据连接程序
 **/
public class MapJoin extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new MapJoin(),args));
    }

    static class JoinMapper extends Mapper<Text, TupleWritable, NullWritable,Text>{
        private NullWritable k2 = NullWritable.get();
        private Text v2 = new Text();

        //将TupleWritable看做是一个容器
        @Override
        protected void map(Text k1, TupleWritable v1, Context context) throws IOException, InterruptedException {
            StringBuffer sb = new StringBuffer();
            sb.append(k1.toString()).append(",");

            Iterator<Writable> it = v1.iterator();
            while (it.hasNext()){
                Writable next = it.next();
                sb.append(next.toString()).append(",");
            }

            this.v2.set(sb.substring(0,sb.length()-1));
            context.write(this.k2,this.v2);
        }
    }

    //reduce端不做处理，使用默认的reduce即可

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();

        Path in1 = new Path(conf.get("in1"));
        Path in2 = new Path(conf.get("in2"));
        Path out = new Path(conf.get("out"));

        //生成每个进行连接数据的map任务的参照标准
        String str = CompositeInputFormat.compose("inner", KeyValueTextInputFormat.class, in1, in2);
        //通过系统配置信息，将该连接的字符串表达式传递到所有的map任务上
        conf.set("mapreduce.join.expr",str);

        Job job = Job.getInstance(conf, "map端join连接");
        job.setJarByClass(this.getClass());

        job.setMapperClass(JoinMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(CompositeInputFormat.class);
        FileInputFormat.addInputPath(job,in1);
        FileInputFormat.addInputPath(job,in2);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job,out);

        return job.waitForCompletion(true)?0:1;
    }
}
