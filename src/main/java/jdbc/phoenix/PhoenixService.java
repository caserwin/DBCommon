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

    public static HashMap<String, String> getTypeMap(){
        return typeMap;
    }
}
