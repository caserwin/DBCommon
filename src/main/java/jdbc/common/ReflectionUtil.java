package jdbc.common;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

/**
 * Created by yidxue on 2018/7/1
 */
public class ReflectionUtil {

    public static <T> String[] getCols(Class<T> clazz) {
        String[] cols = null;
        try {
            String cls = clazz.getField("cls").get(clazz).toString();
            Method m2 = clazz.getMethod("getCols", String.class);
            cols = (String[]) m2.invoke(null, cls);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException | NoSuchFieldException e) {
            e.printStackTrace();
        }
        return cols;
    }

    public static <T> String[] getPrimaryKey(Class<T> clazz) {
        String[] primaryKey = null;
        try {
            String cls = clazz.getField("cls").get(clazz).toString();
            Method m2 = clazz.getMethod("getPrimaryKeys", String.class);
            primaryKey = (String[]) m2.invoke(null, cls);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException | NoSuchFieldException e) {
            e.printStackTrace();
        }
        return primaryKey;
    }

    public static <T> String[] getTypes(Class<T> clazz) {
        String[] types = null;
        try {
            String cls = clazz.getField("cls").get(clazz).toString();
            Method m2 = clazz.getMethod("getTypes", String.class);
            types = (String[]) m2.invoke(null, cls);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException | NoSuchFieldException e) {
            e.printStackTrace();
        }
        return types;
    }

    public static <T> String[] getComments(Class<T> clazz) {
        String[] comments = null;
        try {
            String cls = clazz.getField("cls").get(clazz).toString();
            Method m2 = clazz.getMethod("getComments", String.class);
            comments = (String[]) m2.invoke(null, cls);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException | NoSuchFieldException e) {
            e.printStackTrace();
        }
        return comments;
    }

    public static <T> HashMap<String, String> getColAndType(Class<T> clazz) {
        HashMap<String, String> colAndType = null;
        try {
            String cls = clazz.getField("cls").get(clazz).toString();
            Method m2 = clazz.getMethod("getColAndType", String.class);
            colAndType = (HashMap<String, String>) m2.invoke(null, cls);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException | NoSuchFieldException e) {
            e.printStackTrace();
        }
        return colAndType;
    }

    public static <T> HashMap<String, String> getColAndComment(Class<T> clazz) {
        HashMap<String, String> colAndComment = null;
        try {
            String cls = clazz.getField("cls").get(clazz).toString();
            Method m = clazz.getMethod("getColAndComment", String.class);
            colAndComment = (HashMap<String, String>) m.invoke(null, cls);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | NoSuchFieldException e) {
            e.printStackTrace();
        }
        return colAndComment;
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

    public static <T> T buildFields(Class<T> clazz, LinkedHashMap<String, String> colAndValue) {
        T values = null;
        try {
            Constructor<?> c = clazz.getConstructor();
            Object obj = c.newInstance();
            Method m = clazz.getMethod("buildFields", LinkedHashMap.class);
            values = (T) m.invoke(clazz.cast(obj), colAndValue);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e) {
            e.printStackTrace();
        }
        return values;
    }
}
