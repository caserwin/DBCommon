package jdbc.conn;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @author yidxue
 */
public class DBConnection {

    private static Connection conn = null;

    public static Connection getConnection(String type, String url) {
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


    public static Connection getConnection(String type, String url, String username, String password) {
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


    private static String getDBDriver(String type) {
        switch (type.toLowerCase()) {
            case "hive":
                return URLConstant.HIVE_DRIVER;
            case "mysql":
                return URLConstant.MYSQL_DRIVER;
            case "phoenix":
                return URLConstant.PHOENIX_DRIVER;
            default:
                return URLConstant.MYSQL_DRIVER;
        }
    }
}
