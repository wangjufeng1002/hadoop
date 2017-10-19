package wjf.bigdata.mr.mapsidejoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import wjf.bigdata.mr.join.InfoBean;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author:ju
 * @Description:
 * @Date:Create in 12:20 2017-10-19
 */
public class MapSideJoin {
    static class MapSideJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        Map<String, String> pdInfoMap = new HashMap<String, String>();
        InfoBean bean = new InfoBean();
        Text k = new Text();

        /**
         * 通过阅读Mapper源码，发现setup方法是在maptask 处理数据之前调用一次
         * 可以做一些初始化工作
         *
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("pdts.txt")));
            String line;
            while (StringUtils.isNotEmpty(line = br.readLine())) {
                String[] fields = line.split("\t");
                pdInfoMap.put(fields[0], fields[1]);
            }
            br.close();

        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String orderLine = value.toString();
            String pid = "";
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String name = fileSplit.getPath().getName();
            if (name.startsWith("order")) {
                String[] fields = orderLine.split("\t");
                String pName = pdInfoMap.get(fields[2]);
                k.set(orderLine + "\t" + pName);
                context.write(k, NullWritable.get());
            }

        }
    }

    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance();
        job.setJarByClass(MapSideJoin.class);

        job.setMapperClass(MapSideJoinMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //指定需要缓存一个文件到所有的maptask运行节点工作目录
        //job.addArchiveToClassPath();          // 缓存jar包到task运行节点的classpath中
        //job.addFileToClassPath(new Path());   // 缓存普通文件到task运行节点的classpath中
        //job.addCacheArchive(url);             //  缓存压缩包文件到task运行节点的工作目录中
        //job.addCacheFile(new URI(args[2]));     //缓存普通文件到task运行节点的工作目录中

        //将产品表文件缓存到task工作节点的工作目录中去
        job.addCacheFile(new URI(args[2]));

        //map端join的逻辑不需要reducerTask .设置reducer数量为零
        job.setNumReduceTasks(0);

        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }
}
