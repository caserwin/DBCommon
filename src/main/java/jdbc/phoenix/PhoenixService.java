package jdbc.phoenix;

import java.util.HashMap;

/**
 * Created by yidxue on 2018/7/2
 */
public class PhoenixService {
    private static HashMap<String, String> typeMap = new HashMap<>();

    static {
        typeMap.put("int", "INTEGER");
        typeMap.put("string", "VARCHAR");
        typeMap.put("float", "FLOAT");
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
}
