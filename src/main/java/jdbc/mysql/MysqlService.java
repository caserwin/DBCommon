package jdbc.mysql;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;

/**
 * Created by yidxue on 2018/7/4
 */
public class MysqlService {
    private static HashMap<String, String> typeMap = new HashMap<>();

    static {
        typeMap.put("int", "int(11)");
        typeMap.put("string", "varchar(255)");
        typeMap.put("float", "float(6,2)");
    }

    public static HashMap<String, String> getTypeMap() {
        return typeMap;
    }

    public static boolean isPrimaryKey(String[] pkArray, String col) {
        for (String pk : pkArray) {
            if (col.toLowerCase().equals(pk.toLowerCase())) {
                return true;
            }
        }
        return false;
    }

    public static void setPSTMT(PreparedStatement pstmt, int index, String type, String value) throws SQLException {
        switch (type) {
            case "string":
                pstmt.setString(index, value);
                break;
            case "int":
                pstmt.setInt(index, Integer.valueOf(value));
                break;
            case "float":
                pstmt.setFloat(index, Float.valueOf(value));
                break;
            default:
                break;
        }
    }
}
