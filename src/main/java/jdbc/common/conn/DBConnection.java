package jdbc.common.conn;

import jdbc.common.ConstantUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * @author yidxue
 */
public class DBConnection {

    private Connection conn = null;

    public Connection getConnection(String type, String url) {
        if (null == conn) {
            synchronized (DBConnection.class) {
                if (null == conn) {
                    try {
                        Class.forName(getDBDriver(type));
                        conn = DriverManager.getConnection(url);
                    } catch (SQLException | ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return conn;
    }

    public Connection getConnection(String type, String url, Properties properties) {
        if (null == conn) {
            synchronized (DBConnection.class) {
                if (null == conn) {
                    try {
                        Class.forName(getDBDriver(type));
                        conn = DriverManager.getConnection(url, properties);
                    } catch (SQLException | ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return conn;
    }

    public Connection getConnection(String type, String url, String username, String password) {
        if (null == conn) {
            synchronized (DBConnection.class) {
                if (null == conn) {
                    try {
                        Class.forName(getDBDriver(type));
                        conn = DriverManager.getConnection(url, username, password);
                    } catch (SQLException | ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return conn;
    }


    private String getDBDriver(String type) {
        switch (type.toLowerCase()) {
            case "hive":
                return ConstantUtil.HIVE_DRIVER;
            case "mysql":
                return ConstantUtil.MYSQL_DRIVER;
            case "phoenix":
                return ConstantUtil.PHOENIX_DRIVER;
            default:
                return ConstantUtil.MYSQL_DRIVER;
        }
    }
}
