package wjf.bigdata.mr.sharefriend;

import org.apache.commons.collections.IteratorUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * @Author:ju
 * @Description:
 * @Date:Create in 17:10 2017-10-19
 */
public class ShareFriendsStepOne {
    static class ShareFriendsStepOneMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] person_friends = line.split(":");
            String[] friends = person_friends[1].split(",");
            for (String friend : friends) {
                //输出 好友--人
                context.write(new Text(friend), new Text(person_friends[0]));
            }
        }
    }

    static class ShareFriendsStepOneReducer extends Reducer<Text, Text, Text, Text> {


        @Override
        protected void reduce(Text friend, Iterable<Text> persons, Context context) throws IOException, InterruptedException {
            Text[] texts = (Text[]) IteratorUtils.toArray(persons.iterator());
            StringBuffer sb = new StringBuffer();
            for (Text person : persons) {
                sb.append(person).append(",");
            }
            context.write(friend, new Text(sb.toString()));
        }
    }
}
