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
    public void insert(String tablename, ArrayList<T> record);

    /**
     * 查询数据
     */
    public void select(String tablename, ArrayList<T> record);
}
