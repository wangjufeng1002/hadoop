package wjf.bigdata.mr.sharefriend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Arrays;

/**
 * @Author:ju
 * @Description:
 * @Date:Create in 21:24 2017-10-20
 */
public class ShareFriendsStepTwo {

    static class ShareFriendsStepTwoMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] friend_persons = line.split("\t");
            String friend = friend_persons[0];
            String[] persons = friend_persons[1].split(",");
            //排序，防止出现 A - C   和 C-A这种情况
            Arrays.sort(persons);
            for (int i = 0; i < persons.length - 1; i++) {
                for (int j = i + 1; j < persons.length; j++) {
                    context.write(new Text(persons[i] + "-" + persons[j]), new Text(friend));
                }
            }
        }
    }

    static class ShareFriendsStepTWoReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text person_person, Iterable<Text> friends, Context context) throws IOException, InterruptedException {
            StringBuffer sb = new StringBuffer();
            for (Text friend : friends) {
                sb.append(friend).append(" ");

            }
            context.write(person_person, new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(ShareFriendsStepTwo.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(ShareFriendsStepTwoMapper.class);
        job.setReducerClass(ShareFriendsStepTWoReducer.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

    }
}
