package jdbc.common;

import jdbc.common.tuple.Tuple2;
import jdbc.common.tuple.Tuple3;

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
    <T> void insert(String tablename, Class<T> clazz, ArrayList<BaseRecord> records);

    /**
     * 查询数据
     */
    <T> ArrayList<T> select(String tablename, Class<T> clazz, String[] cols, ArrayList<Tuple3<String, String, String>> conds);

    /**
     * 更新数据
     */
    <T> void update(String tablename, Class<T> clazz, ArrayList<Tuple2<String, String>>  cols, ArrayList<Tuple3<String, String, String>> conds);
}
