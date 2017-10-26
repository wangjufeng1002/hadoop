package wjf.bigdata.mr.logenhance;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import javax.xml.soap.Text;
import java.io.IOException;

/**
 * @Author:ju
 * @Description:
 * @Date:Create in 21:42 2017-10-20
 */
public class LogEnhanceOutputFormat extends FileOutputFormat<Text, NullWritable> {
    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        FileSystem fs = FileSystem.get(context.getConfiguration());
        Path enhancePath = new Path("/log/temp/en/log.dat");
        Path tocrawlPath = new Path("/log/temp/crw/url.dat");

        FSDataOutputStream enhanceOS = fs.create(enhancePath);
        FSDataOutputStream tocrawlOS = fs.create(tocrawlPath);
        return new EnhanceRecoderWrite(enhanceOS, tocrawlOS);
    }

    static class EnhanceRecoderWrite extends RecordWriter<Text, NullWritable> {
        FSDataOutputStream enhancedOS = null;
        FSDataOutputStream tocrawlOS = null;

        public EnhanceRecoderWrite(FSDataOutputStream enhancedOs, FSDataOutputStream tocrawlOs) {
            this.enhancedOS = enhancedOs;
            this.tocrawlOS = tocrawlOs;
        }

        @Override
        public void write(Text key, NullWritable nullWritable) throws IOException, InterruptedException {
            String result = key.toString();
            // 如果要写出的数据是待爬的url，则写入待爬清单文件 /logenhance/tocrawl/url.dat
            if (result.contains("tocrawl")) {
                tocrawlOS.write(result.getBytes());
            } else {
                // 如果要写出的数据是增强日志，则写入增强日志文件 /logenhance/enhancedlog/log.dat
                enhancedOS.write(result.getBytes());
            }
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            if (tocrawlOS != null) {
                tocrawlOS.close();
            }
            if (enhancedOS != null) {
                enhancedOS.close();
            }
        }
    }
}
