package jdbc.common;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Created by yidxue on 2018/6/28
 */
public class ConstantUtil {
    private static Config conf = ConfigFactory.load();

    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    public static final String HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";
    public static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";

    public static String getURL(String dbType) {
        String host;
        String port;
        String dbname;
        String zkAddr;
        switch (dbType.toLowerCase()) {
            case "mysql":
                host = conf.getString("database.connection." + dbType + ".host");
                port = conf.getString("database.connection." + dbType + ".port");
                dbname = conf.getString("database.connection." + dbType + ".dbname");
                return "jdbc:mysql://" + host + ":" + port + "/" + dbname;
            case "hive":
                host = conf.getString("database.connection." + dbType + ".host");
                port = conf.getString("database.connection." + dbType + ".port");
                dbname = conf.getString("database.connection." + dbType + ".dbname");
                return "jdbc:hive2://" + host + ":" + port + "/" + dbname;
            case "phoenix":
                zkAddr = conf.getString("database.connection." + dbType + ".zookeeper.address");
                port = conf.getString("database.connection." + dbType + ".zookeeper.port");
                return "jdbc:phoenix:" + zkAddr + ":" + port;
            default:
                return "";
        }
    }

    public static String getUserName(String dbType) {
        try {
            return conf.getString("database.connection." + dbType + ".username");
        } catch (Exception e) {
            return "";
        }
    }

    public static String getPassWord(String dbType) {
        try {
            return conf.getString("database.connection." + dbType + ".password");
        } catch (Exception e) {
            return "";
        }
    }

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

    public static String[] getPhoenixFamily(String cls) {
        try {
            return conf.getString("database.table." + cls + ".phoenix.family").toLowerCase().split(",");
        } catch (Exception e) {
            return null;
        }
    }

    public static int getPhoenixTTL() {
        try {
            return Integer.parseInt(conf.getString("database.connection.phoenix.ttl").trim());
        } catch (Exception e) {
            return 0;
        }
    }

    public static int getPhoenixBUCKETS() {
        try {
            return Integer.parseInt(conf.getString("database.connection.phoenix.salt_buckets").trim());
        } catch (Exception e) {
            return 100;
        }
    }
}
