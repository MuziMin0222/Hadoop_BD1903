package com.MapReduce_Join.DataBaseWriteAndReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @program:Hadoop_BD1903
 * @package:com.MapReduce_Join.DataBaseWriteAndReader
 * @filename:HdfsToDatabases.java
 * @create:2019.10.09.19:52:57
 * @auther:李煌民
 * @description:.从集群中读取文件到数据库中
 **/
public class HdfsToDatabases extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new HdfsToDatabases(),args));
    }

    static class HdfsMapper extends Mapper<LongWritable, Text,YearSidTemp, NullWritable>{
        private YearSidTemp k2 = new YearSidTemp();
        private NullWritable v2 = NullWritable.get();

        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
            String[] strs = v1.toString().split("[\t]");
            this.k2.setYear(Integer.parseInt(strs[0]));
            this.k2.setSid(strs[1]);
            this.k2.setTemp(Double.parseDouble(strs[2]));
            context.write(k2,v2);
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();

        Path in = new Path(conf.get("in"));

        Job job = Job.getInstance(conf, "从集群中到数据库");
        job.setJarByClass(this.getClass());

        job.setMapperClass(HdfsMapper.class);
        job.setMapOutputKeyClass(YearSidTemp.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job,in);

        job.setOutputKeyClass(YearSidTemp.class);
        job.setOutputValueClass(NullWritable.class);
        //设置输出格式为输出到数据库中
        job.setOutputFormatClass(DBOutputFormat.class);
        //配置数据库的连接信息
        DBConfiguration.configureDB(job.getConfiguration(),
                "com.mysql.cj.jdbc.Driver",
                "jdbc:mysql://bd1:3306/bd1903",
                "root",
                "root");
        //配置数据输出的信息
        DBOutputFormat.setOutput(job,"tb_yst","year","sid","temp");

        return job.waitForCompletion(true)?0:1;
    }
}
