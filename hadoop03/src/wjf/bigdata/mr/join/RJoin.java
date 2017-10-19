package wjf.bigdata.mr.join;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @Author:ju
 * @Description:
 * @Date:Create in 19:44 2017-10-18
 */
public class RJoin {
    static class RJoinMapper extends Mapper<LongWritable, Text, Text, InfoBean> {
        InfoBean bean = new InfoBean();
        Text k = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();

            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            String name = inputSplit.getPath().getName();
            // 通过文件名判断是哪种数据
            String pid = "";
            if (name.startsWith("order")) {
                String[] fields = line.split("\t");
                // id date pid amount
                pid = fields[2];
                bean.set(Integer.parseInt(fields[0]), fields[1], pid, Integer.parseInt(fields[3]), "", 0, 0, "0");

            } else {
                String[] fields = line.split("\t");
                // id pname category_id price
                pid = fields[0];
                bean.set(0, "", pid, 0, fields[1], Integer.parseInt(fields[2]), Float.parseFloat(fields[3]), "1");

            }
            k.set(pid);
            context.write(k, bean);
        }

    }

    static class RJoinReducer extends Reducer<Text, InfoBean, InfoBean, NullWritable> {
        @Override
        protected void reduce(Text pid, Iterable<InfoBean> beans, Context context) throws IOException, InterruptedException {
            InfoBean pdBean = new InfoBean();
            ArrayList<InfoBean> orderBeans = new ArrayList<InfoBean>();

            for (InfoBean bean : beans) {
                if ("1".equals(bean.getFlag())) {    //产品的
                    try {
                        BeanUtils.copyProperties(pdBean, bean);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } else {
                    InfoBean odbean = new InfoBean();
                    try {
                        BeanUtils.copyProperties(odbean, bean);
                        orderBeans.add(odbean);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }

            // 拼接两类数据形成最终结果
            for (InfoBean bean : orderBeans) {
                bean.setPname(pdBean.getPname());
                bean.setCategory_id(pdBean.getCategory_id());
                bean.setPrice(pdBean.getPrice());
                context.write(bean, NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        //要想运行为集群模式，一下三个参数要设置集群上的值
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resoucemanage.hostname", "server02");
        conf.set("fs.defaultFS", "hdfs://server02:9000/");
        Job job = Job.getInstance(conf);
        //指定本程序jar包所在的路径
        job.setJarByClass(RJoin.class);

        //指定本业务job要使用的maopper/Reducer业务类
        job.setMapperClass(RJoinMapper.class);
        job.setReducerClass(RJoinReducer.class);

        //制定mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(InfoBean.class);

        //指定最终输出的数据类型kv类型
        job.setOutputKeyClass(InfoBean.class);
        job.setOutputValueClass(NullWritable.class);

        //指定需要使用combiner ,以及用哪个类作为combiner的逻辑
        //job.setCombinerClass(WordcountCombiner.class);
        //job.setCombinerClass(WordcountReducer.class);

        //指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        //指定job输出结果所在目录
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
        /*job.submit();*/
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }
}
