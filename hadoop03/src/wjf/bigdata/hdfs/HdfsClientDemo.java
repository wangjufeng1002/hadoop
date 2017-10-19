package wjf.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.Map;

public class HdfsClientDemo {
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
    public void testUpload() throws IllegalArgumentException, IOException {
        fs.copyFromLocalFile(new Path("h://templet.xlsx"), new Path("/test.java"));
        fs.close();
    }

    @Test
    public void testConf() {
        Iterator<Map.Entry<String, String>> confIterator = conf.iterator();
        while (confIterator.hasNext()) {
            Map.Entry<String, String> next = confIterator.next();
            System.out.println(next.getKey() + " ----> " + next.getValue());
        }
    }

    @Test
    public void testMkdir() throws IOException {
        boolean mkdirs = fs.mkdirs(new Path("/wang/ju/feng"));
        System.out.println(mkdirs);
    }

    @Test
    public void testDelete() throws IOException {
        boolean delete = fs.delete(new Path("/wang/ju/feng"), true);
        System.out.println(delete);
    }

    @Test
    public void testLs1() throws IOException {
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), true);
        while (files.hasNext()) {
            LocatedFileStatus fileStatus = files.next();
            System.out.println("blockSize: " + fileStatus.getBlockSize());
            System.out.println("Group    : " + fileStatus.getGroup());
            System.out.println("name     : " + fileStatus.getPath());
        }
    }

    @Test
    public void testLs2() throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus file : fileStatuses) {
            System.out.println("name: " + file.getPath().getName());
            System.out.println(file.isFile() ? "file" : "directory");
        }
    }

    @Test
    public void testDownLoad() throws IOException {
        fs.copyToLocalFile(new Path("/join/input/order.txt"), new Path("H://order.txt"));
        fs.copyToLocalFile(new Path("/join/input/product.txt"), new Path("H://product.txt"));

    }

}
