package com.MapReduce_Join.DataBaseWriteAndReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @program:Hadoop_BD1903
 * @package:com.MapReduce_Join.DataBaseWriteAndReader
 * @filename:DatabasesToHdfs.java
 * @create:2019.10.09.21:01:33
 * @auther:李煌民
 * @description:.从数据库中读取数据到集群中
 **/
public class DatabasesToHdfs extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new DatabasesToHdfs(),args));
    }

    static class DBMapper extends Mapper<LongWritable, YearSidTemp, IntWritable,Text>{
        private IntWritable k2 = new IntWritable();
        private Text v2 = new Text();

        @Override
        protected void map(LongWritable k1, YearSidTemp v1, Context context) throws IOException, InterruptedException {
            this.k2.set(v1.getYear().get());
            this.v2.set(v1.getSid().toString() + "\t" + v1.getTemp().get());
            context.write(this.k2,this.v2);
        }
    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();

        Path out = new Path(conf.get("out"));

        Job job = Job.getInstance(conf, "从数据库中读取数据写到集群中");
        job.setJarByClass(DatabasesToHdfs.class);

        job.setMapperClass(DBMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(DBInputFormat.class);
        //配置数据连接信息
        DBConfiguration.configureDB(job.getConfiguration(),
                "com.mysql.cj.jdbc.Driver",
                "jdbc:mysql://bd1:3306/bd1903",
                "root","root" );
        // select year,sid,temp
        // from tbl_yst
        // where year>1995
        // order by temp
        DBInputFormat.setInput(job,
                YearSidTemp.class,
                "tb_yst",
                "temp >= 100",
                "temp",
                "year","sid","temp");

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,out);

        return job.waitForCompletion(true)?0:1;
    }
}
