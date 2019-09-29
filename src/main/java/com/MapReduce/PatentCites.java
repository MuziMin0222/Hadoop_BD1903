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
 * @package:com.com.MapReduce
 * @filename:PatentCites.java
 * @create:2019.09.26.16:35:21
 * @auther:李煌民
 * @description:.使用MapReduce程序计算专利引用，计算每个专利都引用了哪些专利
 *
 * 1、编写map任务
 * 2、编写reduce任务
 * 3、作业配置，都是配置在run方法中
 * 整个MR程序运行的时候，run方法中的代码会在客户端执行
 * 编写的map任务和reduce任务会在集群中执行
 * 所以在run方法出现的打印语句可以打印在控制台上，而在Map或者reduce任务中出现的打印语句不会打印在控制台上
 * 而是以日志形式记录在日志文件中
 **/
public class PatentCites extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new PatentCites(),args));
    }

    //1、执行map任务
    static class PatentCitesMapper extends Mapper<LongWritable, Text,Text,Text>{
        private Text k2 = new Text();
        private Text v2 = new Text();

        @Override
        protected void map(LongWritable k1,//读取到一行数据的偏移量
                           Text v1,//读取到一行数据value
                           Context context//联系map任务和MapReduce框架的对象
        ) throws IOException, InterruptedException {
            String[] strs = v1.toString().split("[,]");
            this.k2.set(strs[0]);
            this.v2.set(strs[1]);
            //交给reduce
            context.write(this.k2,this.v2);
        }
    }

    //2、编写reduce任务
    static class PatentCitesRedece extends Reducer<Text,Text,Text,Text>{
        private Text k3 = new Text();
        private Text v3 = new Text();

        @Override
        protected void reduce(Text k2,     //map端出来的k2
                              Iterable<Text> v2s,    //map端出来的v2
                              Context context   //联系MapReduce和reduce框架的对象
        ) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            v2s.forEach(v2->{
                sb.append(v2.toString()).append(",");
            });

            this.k3.set(k2.toString());
            this.v3.set(sb.substring(0,sb.length()-1));

            context.write(this.k3,this.v3);
        }
    }

    //3、作业配置
    @Override
    public int run(String[] strings) throws Exception {
        //将程序打包至客户端(集群中的任意一个节点)上运行的时候，下面的代码会根据客户端配置会自动读取配置文件
        Configuration conf = this.getConf();

        //文件输入路径
        Path in = new Path(conf.get("in"));
        //文件输出路径
        Path out = new Path(conf.get("out"));
        //关于MapReduce程序运行时的结果存放路径在hdfs集群上一定不能预先存在，如果存在，那么该程序报异常
        //MR程序的运行结果会被存放在指定的目录中
        //mr程序处理数据的结果会生成多个文件：_SUCCESS,part-r-00000
        //Map任务的个数，与数据分片有关，也就是和数据块有关
        //reduce任务的个数：默认是一个，reduce的任务是可以配置的
        //命令行配置 > 代码中的配置 > 配置文件中的配置 > 默认
        //有多少个reduce，就有多少个结果文件，不一定所有的reduce都会参与数据的运算
        //reduce处理的结果文件中，有些文件有可能是空的

        Job job = Job.getInstance(conf, "专利引用");
        job.setJarByClass(this.getClass());

        //Map任务的配置
        //配置map任务要运行的类
        job.setMapperClass(PatentCitesMapper.class);
        //配置map任务输出的k2和v2的数据类型，因为在输出的时候MapReduce框架不知道输出的什么类型，所以要指明输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //配置Map任务读取原始数据的格式，即配置map任务以什么样的方式读取原始数据
        //该配置决定了数据进入map端的k1和v1的数据类型
        job.setInputFormatClass(TextInputFormat.class);
        //配置文件的输入路径
        TextInputFormat.addInputPath(job,in);

        //Reduce任务的配置
        //配置Reduce任务要运行的类
        job.setReducerClass(PatentCitesRedece.class);
        //配置Reduce任务输出的k3和v3的数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //配置Reduce任务输出原始数据的格式，即配置Reduce任务以什么样的输出原始数据
        job.setOutputFormatClass(TextOutputFormat.class);
        //配置文件的输出路径
        TextOutputFormat.setOutputPath(job,out);

        //设置Combiner类,因为reduce的逻辑代码和Combiner的逻辑代码一致，所以直接用reduce的类
        job.setCombinerClass(PatentCitesRedece.class);

        System.out.println("任务配置完成");

        //提交作业
        return job.waitForCompletion(true)?0:1;
    }
}
