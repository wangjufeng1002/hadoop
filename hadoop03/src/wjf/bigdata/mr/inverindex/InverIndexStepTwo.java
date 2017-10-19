package wjf.bigdata.mr.inverindex;

/**
 * @Author:ju
 * @Description:
 * @Date:Create in 16:58 2017-10-19
 */

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InverIndexStepTwo {
    public static class InverIndexStepTwoMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] files = line.split("--");
            context.write(new Text(files[0]), new Text(files[1]));
        }
    }

    public static class InverIndexStepTwoReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer sb = new StringBuffer();
            for (Text text : values) {
                sb.append(text.toString().replace("\t", "-->") + "\t");
            }
            context.write(key, new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws Exception {


        Configuration config = new Configuration();
        Job job = Job.getInstance(config);
        job.setJarByClass(InverIndexStepTwo.class);

        job.setMapperClass(InverIndexStepTwoMapper.class);
        job.setReducerClass(InverIndexStepTwoReducer.class);

//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 1 : 0);
    }
}
