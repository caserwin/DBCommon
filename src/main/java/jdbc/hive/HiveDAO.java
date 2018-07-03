package jdbc.hive;

import jdbc.DBOperate;
import jdbc.conn.DBConnection;
import jdbc.service.FileUtil;
import jdbc.service.ReflectionService;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.stream.Collectors;

/**
 * @author yidxue
 */
public class HiveDAO implements DBOperate<Object>{

    private String URLHIVE = "jdbc:hive2://localhost:10000/default";
    private String DBType = "hive";
    private Connection conn;

    public HiveDAO() {
        this.conn = DBConnection.getConnection(DBType, URLHIVE);
    }

    @Override
    public <T> void create(String tablename, Class<T> clazz) {
        try {
            HashMap<String, String> colAndType = ReflectionService.getColAndType(clazz);
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
    public <T> void insert(String tablename, Class<T> clazz, ArrayList<Object> record) {

    }

    @Override
    public <T> void select(String tablename, Class<T> clazz) {

    }

    public <T> void loadToHive(ArrayList<T> records, Class<T> clazz, String tableName, String path) {
        try {
            FileUtil.writeByStream(records.stream().map(x -> ReflectionService.getValues(x, clazz).stream().collect(Collectors.joining("\t"))).collect(Collectors.toList()), path);
            // 每一层目录都设置 777 权限
            FileUtil.changeFolderPermission(path, true);
            Statement stmt = this.conn.createStatement();
            String sql = "LOAD DATA LOCAL INPATH '" + path + "' into table " + tableName;
            System.out.println(sql);
            stmt.execute(sql);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
