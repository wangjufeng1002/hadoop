package wjf.bigdata.mr.logenhance;

import java.util.Map;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author:ju
 * @Description:
 * @Date:Create in 21:37 2017-10-20
 */
public class DBLoader {
    public static void dbLoader(Map<String, String> ruleMap) {
        Connection conn = null;
        Statement st = null;
        ResultSet res = null;

        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://local:3306/urldb", "root", "root");
            st = conn.createStatement();
            res = st.executeQuery("select url,content from url_rule");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (res != null) {
                    res.close();
                }
                if (st != null) {
                    st.close();
                }
                if (conn != null) {
                    conn.close();
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
