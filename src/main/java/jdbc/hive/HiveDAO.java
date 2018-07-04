package jdbc.hive;

import jdbc.common.DBOperate;
import jdbc.common.FileUtil;
import jdbc.common.ReflectionUtil;
import jdbc.common.tuple.Tuple2;
import jdbc.common.tuple.Tuple3;
import jdbc.conn.DBConnection;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author yidxue
 */
public class HiveDAO implements DBOperate<Object> {

    private String URLHIVE = "jdbc:hive2://10.29.42.49:10000/default";
    private String DBType = "hive";
    private Connection conn;

    public HiveDAO() {
        this.conn = DBConnection.getConnection(DBType, URLHIVE);
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
    public <T> void insert(String tablename, Class<T> clazz, ArrayList<T> record) {

    }

    @Override
    public <T> ArrayList<T> select(String tablename, Class<T> clazz, String[] cols, ArrayList<Tuple3<String, String, String>> conds) {
        ArrayList<T> recordLS = new ArrayList<>();
        String fields = Stream.of(cols).collect(Collectors.joining(","));
        HashMap<String, String> colAndType = ReflectionUtil.getColAndType(clazz);
        String sql = "SELECT " + fields + " FROM " + tablename;
        if (conds != null && conds.size() != 0) {
            String condStr = conds.stream().map(x -> {
                if ("string".equals(colAndType.get(x.column.toLowerCase()).toLowerCase())) {
                    return x.column + " " + x.operator + " " + "'" + x.value + "'";
                } else {
                    return x.column + " " + x.operator + " " + x.value;
                }
            }).collect(Collectors.joining(" and "));
            sql = sql + " where " + condStr;
        }
        System.out.println(sql);
        try {
            Statement stmt = this.conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                LinkedHashMap<String, String> colAndValue = new LinkedHashMap<>();
                for (String col : cols) {
                    colAndValue.put(col, rs.getString(col));
                }
                recordLS.add(ReflectionUtil.buildFields(clazz, colAndValue));
            }
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return recordLS;
    }

    @Override
    public <T> void update(String tablename, Class<T> clazz, ArrayList<Tuple2<String, String>> cols, ArrayList<Tuple3<String, String, String>> cond) {

    }


    public <T> void loadToHive(ArrayList<T> records, Class<T> clazz, String tableName, String path) {
        try {
            FileUtil.writeByStream(records.stream().map(x -> ReflectionUtil.getValues(x, clazz).stream().collect(Collectors.joining("\t"))).collect(Collectors.toList()), path);
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
