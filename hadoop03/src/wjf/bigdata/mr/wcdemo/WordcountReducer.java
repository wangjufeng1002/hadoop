package wjf.bigdata.mr.wcdemo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * KEYIN, VALUEIN 对应 mapper 中输出的 KEYOUT,VALUEOUT 类型对应的数据
 */

public class WordcountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     * 入参key ,是一组想同单词kv对的key
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;

        for (IntWritable value : values) {
            count += value.get();
        }
        context.write(key, new IntWritable(count));
    }
}
