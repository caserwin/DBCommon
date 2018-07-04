package jdbc.common;

import java.util.ArrayList;

/**
 * Created by yidxue on 2018/6/30
 */
public interface DBOperate<T> {
    /**
     * 建表
     */
    <T> void create(String tablename, Class<T> clazz);

    /**
     * 插入数据
     */
    <T> void insert(String tablename, Class<T> clazz, ArrayList<Object> record);

    /**
     * 查询数据
     */
    <T> ArrayList<T> select(String tablename, Class<T> clazz, String[] cols, ArrayList<Tuple3<String, String, String>> cond);

    /**
     * 更新数据
     */
    <T> void update(String tablename, Class<T> clazz, String[] cols, ArrayList<Tuple3<String, String, String>> cond);
}
