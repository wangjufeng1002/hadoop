package wjf.bigdata.mr.wcdemo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author:ju
 * @Description:
 * @Date:Create in 16:22 2017-10-18
 */
public class WordcountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable v : values) {
            count += v.get();
        }
        context.write(key, new IntWritable(count));
    }
}
