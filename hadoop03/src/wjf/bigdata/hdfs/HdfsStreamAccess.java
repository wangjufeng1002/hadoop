package wjf.bigdata.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsStreamAccess {

    private FileSystem fs;
    Configuration conf;

    @Before
    public void init() throws IOException, InterruptedException, URISyntaxException {
        conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.92.112:9000");
        //拿到一个文件操作的客户端实例对象
        // fs = FileSystem.get(conf);
        fs = FileSystem.get(new URI("hdfs://192.168.92.112:9000"), conf, "hadhoop");
    }

    @Test
    public void testUpload() throws IOException {
        FSDataOutputStream outputStream = fs.create(new Path("/angelababy.love"), true);
        FileInputStream inputStream = new FileInputStream("h:/angelababy.love");

        int copy = IOUtils.copy(inputStream, outputStream);
    }
    @Test
    public void testDownLoac() throws IOException {
        FSDataInputStream inputStream = fs.open(new Path("/angelababy.love"));

        FileOutputStream outputStream = new FileOutputStream("e:/angelababy.love");
        IOUtils.copy(inputStream, outputStream);

    }
    @Test
    public void testRandomAccess() throws IOException {
        FSDataInputStream inputStream = fs.open(new Path("/angelababy.love"));

        inputStream.seek(30);

        FileOutputStream outputStream = new FileOutputStream("e:/angelababy.love1");

        IOUtils.copy(inputStream, outputStream);

    }

    @Test
    public void testCat() throws IOException {
        FSDataInputStream inputStream = fs.open(new Path("/flowsum/output/part-r-00000"));

        IOUtils.copy(inputStream, System.out);
    }
}
