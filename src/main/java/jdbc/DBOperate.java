package jdbc;

import java.util.ArrayList;

/**
 * Created by yidxue on 2018/6/30
 */
public interface DBOperate<T> {
    /**
     * 建表
     */
    public <T> void create(String tablename, Class<T> clazz);

    /**
     * 插入数据
     */
    public <T> void insert(String tablename, Class<T> clazz, ArrayList<Object> record);

    /**
     * 查询数据
     */
    public <T> void select(String tablename, Class<T> clazz);
}
