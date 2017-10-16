package main.java.wjf.bigdata.mr.wcdemo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEYIN 默认情况下，是mr框架所读到的一行文本的起始偏移量 Long，但是在hadoop中有自己的更精简的序列化接口，所以不直接用long,而是用LongWritable
 *
 * VALUE:默认情况下，是mr框架所读到的一行文本内容，String。同上采用Text
 *
 * KEYOUT:是用户自定义逻辑处理完成之后输出数据中的key。再此处是单词，所以同上使用Text
 *
 * VALUEOUT : 是用户自定义逻辑处理完成之后输出数据中的value，在此处是单词次数，Integer,同上使用IntWritable
 */
public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    /**
     * map 阶段的业务逻辑就写在自定义的map（）方法中。
     * maptask 会对每一行输入数据调用一次我们自定义的map()方法。
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //将maptask 传递给我们的文本内容先转换成String
        String line = value.toString();
        //根据空格，将这一行切成单词
        String[] words = line.split(" ");

        //将单词输出为<单词，1>
        for(String word :words){
            //将单词作为key，将次数1作为value ，以便于后续的数据分发，可以根据单词分发，以便于相同单词会到相同的reduce task
            context.write(new Text(word),new IntWritable(1));
        }



    }
}
