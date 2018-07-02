package jdbc.service;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by yidxue on 2018/7/1
 */
public class ReflectionService {

    public static <T> String[] getCols(Class<T> clazz) {
        String[] cols = null;
        try {
            Method m2 = clazz.getMethod("getCols");
            cols = (String[]) m2.invoke(null);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            e.printStackTrace();
        }
        return cols;
    }

    public static <T> String[] getPrimaryKey(Class<T> clazz) {
        String[] primaryKey = null;
        try {
            Method m2 = clazz.getMethod("getPrimaryKey");
            primaryKey = (String[]) m2.invoke(null);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            e.printStackTrace();
        }
        return primaryKey;
    }

    public static <T> String[] getTypes(Class<T> clazz) {
        String[] types = null;
        try {
            Method m2 = clazz.getMethod("getTypes");
            types = (String[]) m2.invoke(null);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            e.printStackTrace();
        }
        return types;
    }

    public static <T> HashMap<String, String> getColAndType(Class<T> clazz) {
        HashMap<String, String> colAndType = null;
        try {
            Method m2 = clazz.getMethod("getColAndType");
            colAndType = (HashMap<String, String>) m2.invoke(null);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            e.printStackTrace();
        }
        return colAndType;
    }

    public static <T> HashMap<String, String> getColAndValue(Object obj, Class<T> clazz) {
        HashMap<String, String> colAndValue = null;
        try {
            Method m = clazz.getMethod("getColAndValue");
            colAndValue = (HashMap<String, String>) m.invoke(clazz.cast(obj));
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
        }
        return colAndValue;
    }

    public static <T> ArrayList<String> getValues(Object obj, Class<T> clazz) {
        ArrayList<String> values = null;
        try {
            Method m = clazz.getMethod("getValues");
            values = (ArrayList<String>) m.invoke(clazz.cast(obj));
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
        }
        return values;
    }
}
