package wjf.bigdata.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
/**
 * @Author:ju
 * @Description:
 * @Date:Create in 12:14 2017-10-25
 */
public class ToLowerCase extends UDF {

    public String evaluate(String field) {

        String result = field.toLowerCase();

        return result;
    }
}
