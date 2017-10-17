package wjf.bigdata.mr.flowsum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author:ju
 * @Description:
 * @Date:Create in 19:11 2017-10-17
 */
public class FlowCountSort {
    static class FlowCountSortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
        FlowBean flowBean = new FlowBean();
        Text vText = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //拿到的是上一个统计程序的输出结果，已经是各个手机号的总流量信息
            String line = value.toString();
            String[] fields = line.split("\t");
            String phoneNbr = fields[0];
            long upFlow = Long.parseLong(fields[1]);
            long dFlow = Long.parseLong(fields[2]);
            flowBean.set(upFlow, dFlow);
            vText.set(phoneNbr);
            context.write(flowBean, vText);
        }
    }

    static class FlowCountSortReducer extends Reducer<FlowBean, Text, Text, FlowBean> {
        @Override
        protected void reduce(FlowBean bean, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(values.iterator().next(), bean);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance();
        job.setJarByClass(FlowCountSort.class);

        //指定本业务job要使用的mapper类
        job.setMapperClass(FlowCountSortMapper.class);
        job.setReducerClass(FlowCountSortReducer.class);
        //指定map的输出类型是
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);


        //指定最终输出的数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);


        //指定job的输入数据的kv类型
        FileInputFormat.setInputPaths(job, new Path(args[0]));


        Path outPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);
        boolean exists = fs.exists(outPath);
        if (exists) {
            fs.delete(outPath, true);
        }
        //指定job的输出数据的kv类型
        FileOutputFormat.setOutputPath(job, outPath);

        //将job中配置的相关参数，以及
        /*job.submit();*/
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);

    }

}
