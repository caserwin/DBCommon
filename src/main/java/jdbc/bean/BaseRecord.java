package jdbc.bean;

import jdbc.conn.URLConstant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.stream.Collectors;

/**
 * Created by yidxue on 2018/7/2
 */
public abstract class BaseRecord {

    public static String cls;
    private static String[] primaryKey = URLConstant.getPrimaryKey(cls);
    private static String[] cols = URLConstant.getCols(cls);
    private static String[] types = URLConstant.getType(cls);
    private static LinkedHashMap<String, String> colAndType = new LinkedHashMap<>();
    private LinkedHashMap<String, String> colAndValue;

    static {
        for (int i = 0; i < cols.length; i++) {
            colAndType.put(cols[i], types[i]);
        }
    }

    public BaseRecord() {
        colAndValue = new LinkedHashMap<>();
    }

    public abstract BaseRecord buildFields(String... args);

    public void setColAndValue(String field, String value) {
        colAndValue.put(field, value);
    }

    public HashMap<String, String> getColAndValue() {
        return colAndValue;
    }

    public static String[] getPrimaryKey() {
        return primaryKey;
    }

    public static String[] getCols() {
        return cols;
    }

    public static String[] getTypes() {
        return types;
    }

    public static HashMap<String, String> getColAndType() {
        return colAndType;
    }

    public ArrayList<String> getValues() {
        return new ArrayList<>(colAndValue.values());
    }

    @Override
    public String toString() {
        return colAndValue.values().stream().collect(Collectors.joining("\t"));
    }
}

