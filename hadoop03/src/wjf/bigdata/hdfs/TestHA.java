package wjf.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * @Author:ju
 * @Description:
 * @Date:Create in 19:44 2017-10-21
 */
public class TestHA {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://bi");


        FileSystem fs = FileSystem.get(conf);
        fs.copyFromLocalFile(new Path("H://text.txt"), new Path("/testHA"));
        fs.close();

    }
}
