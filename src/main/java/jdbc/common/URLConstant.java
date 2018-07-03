package jdbc.common;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Created by yidxue on 2018/6/28
 */
public class URLConstant {
    private static Config conf = ConfigFactory.load();

    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    public static final String HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";
    public static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";



    public static String[] getPrimaryKey(String cls) {
        return conf.getString("database.table." + cls + ".primarykey").toLowerCase().split(",");
    }

    public static String[] getCols(String cls) {
        return conf.getString("database.table." + cls + ".cols").toLowerCase().split(",");
    }

    public static String[] getType(String cls) {
        return conf.getString("database.table." + cls + ".type").toLowerCase().split(",");
    }
}
