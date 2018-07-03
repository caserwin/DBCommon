package jdbc.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.stream.Collectors;

/**
 * Created by yidxue on 2018/7/2
 * 心得：
 * 1. 属于类的方法的参数是不该通过一个对象实例来传进来的。因为和new 对象没有关系。
 * 2. 一个对象实例也确实不应该看到类的方法。
 */
public abstract class BaseRecord {

    private LinkedHashMap<String, String> colAndValue;

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

    public static String[] getPrimaryKey(String cls) {
        return URLConstant.getPrimaryKey(cls);
    }

    public static String[] getCols(String cls) {
        return URLConstant.getCols(cls);
    }

    public static String[] getTypes(String cls) {
        return URLConstant.getType(cls);
    }

    public static HashMap<String, String> getColAndType(String cls) {
        LinkedHashMap<String, String> colAndType = new LinkedHashMap<>();
        String[] cols = getCols(cls);
        String[] types = getTypes(cls);
        for (int i = 0; i < cols.length; i++) {
            colAndType.put(cols[i], types[i]);
        }
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

