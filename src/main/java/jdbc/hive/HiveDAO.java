package jdbc.hive;

import jdbc.common.*;
import jdbc.common.conn.DBConnection;
import jdbc.common.tuple.Tuple2;
import jdbc.common.tuple.Tuple3;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.stream.Collectors;

/**
 * @author yidxue
 */
public class HiveDAO implements DBOperate<Object> {

    private String dbType = "hive";
    private String hiveURL = ConstantUtil.getURL(dbType);
    private Connection conn;

    public HiveDAO() {
        this.conn = new DBConnection().getConnection(dbType, hiveURL);
    }

    @Override
    public <T> void create(String tablename, Class<T> clazz) {
        try {
            HashMap<String, String> colAndType = ReflectionUtil.getColAndType(clazz);
            String fields = colAndType.entrySet().stream().map(x -> x.getKey() + "\t" + x.getValue()).collect(Collectors.joining(","));
            Statement stmt = this.conn.createStatement();
            String sql = "create table if not exists " + tablename + "(" + fields + ")"
                             + " row format delimited fields terminated by '\\t'"
                             + " collection items terminated by ','";

            System.out.println(sql);
            stmt.execute(sql);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public <T> void insert(String tablename, Class<T> clazz, ArrayList<BaseRecord> record, boolean ifIgnoreDuplicateKey) {

    }

    @Override
    public <T> ArrayList<T> select(String tablename, Class<T> clazz, String[] cols, ArrayList<Tuple3<String, String, String>> conds) {
        return SQLUtil.select(this.conn, tablename, clazz, cols, conds);
    }

    @Override
    public <T> int update(String tablename, Class<T> clazz, ArrayList<Tuple2<String, String>> cols, ArrayList<Tuple3<String, String, String>> conds) {
        return 0;
    }

    @Override
    public <T> int delete(String tablename, Class<T> clazz, ArrayList<Tuple3<String, String, String>> conds) {
        return 0;
    }


    public <T> void loadToHive(ArrayList<BaseRecord> records, Class<T> clazz, String tableName, String path, boolean ifLocal, boolean ifOverride) {
        // LOAD DATA [LOCAL] INPATH 'filepath' [OVERWRITE] INTO TABLE tablename [PARTITION (partcol1=val1, partcol2=val2 ...)]
        // PARTITION 语法可以以后实现。
        try {
            FileUtil.writeByStream(records.stream().map(x -> ReflectionUtil.getValues(x, clazz).stream().collect(Collectors.joining("\t"))).collect(Collectors.toList()), path);
            // 每一层目录都设置 777 权限
            FileUtil.changeFolderPermission(path, true);
            Statement stmt = this.conn.createStatement();
            String override = ifOverride ? "OVERWRITE" : "";
            String local = ifLocal ? "LOCAL" : "";
            String sql = "LOAD DATA " + local + " INPATH '" + path + "' " + override + " into table " + tableName;
            System.out.println(sql);
            stmt.execute(sql);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
