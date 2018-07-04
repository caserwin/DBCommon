package jdbc.common;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.lang.StringUtils;

/**
 * Created by yidxue on 2018/6/28
 */
public class ConstantUtil {
    private static Config conf = ConfigFactory.load();

    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    public static final String HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";
    public static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";

    public static String[] getPrimaryKeys(String cls) {
        return conf.getString("database.table." + cls + ".primarykey").toLowerCase().split(",");
    }

    public static String[] getCols(String cls) {
        return conf.getString("database.table." + cls + ".cols").toLowerCase().split(",");
    }

    public static String[] getTypes(String cls) {
        return conf.getString("database.table." + cls + ".type").toLowerCase().split(",");
    }

    public static String[] getComments(String cls) {
        try {
            return conf.getString("database.table." + cls + ".comment").toLowerCase().split(",");
        } catch (Exception e) {
            return null;
        }
    }
}
