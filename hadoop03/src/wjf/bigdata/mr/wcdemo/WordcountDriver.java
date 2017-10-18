package wjf.bigdata.mr.wcdemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 相当于一个yarn集群的客户端
 * 需要再次封装问哦们的mrc程序的相关运行参数，制定jar
 * 最后提交给yarn
 *
 * @author ju
 */
public class WordcountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resoucemanage.hostname", "server02");
        Job job = Job.getInstance();
        //指定本程序jar包所在的路径
        job.setJarByClass(WordcountDriver.class);

        //指定本业务job要使用的maopper/Reducer业务类
        job.setMapperClass(WordcountMapper.class);
        job.setReducerClass(WordcountReducer.class);

        //制定mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //指定最终输出的数据类型kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //指定需要使用combiner ,以及用哪个类作为combiner的逻辑
        //job.setCombinerClass(WordcountCombiner.class);
        job.setCombinerClass(WordcountReducer.class);

        //指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path("/wordcount/input"));
        //指定job输出结果所在目录
        FileOutputFormat.setOutputPath(job, new Path("/wordcount/output"));
        //将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
        /*job.submit();*/
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }
}
