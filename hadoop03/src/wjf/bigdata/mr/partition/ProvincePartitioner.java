package wjf.bigdata.mr.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;


/**
 * @Author:ju
 * @Description:
 * @Date:Create in 12:41 2017-10-17
 */
public class ProvincePartitioner extends Partitioner<Text, FlowBean> {

    public static HashMap<String, Integer> proviceDict = new HashMap<String, Integer>();

    static {
        proviceDict.put("136", 0);
        proviceDict.put("137", 1);
        proviceDict.put("138", 2);
        proviceDict.put("139", 3);
    }

    @Override
    public int getPartition(Text key, FlowBean flowBean, int i) {
        String prefix = key.toString().substring(0, 3);
        Integer provinceId = proviceDict.get(prefix);
        return provinceId == null ? 4 : provinceId;
    }
}
