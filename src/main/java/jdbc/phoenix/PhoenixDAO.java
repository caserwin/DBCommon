package jdbc.phoenix;

import jdbc.common.*;
import jdbc.common.tuple.Tuple2;
import jdbc.common.tuple.Tuple3;
import jdbc.common.conn.DBConnection;
import jdbc.mysql.MysqlService;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by yidxue on 2018/6/28
 */
public class PhoenixDAO implements DBOperate<Object> {

    private String dbType = "phoenix";
    private String phoenixURL = ConstantUtil.getURL(dbType);
    private Properties properties = new Properties();
    private Connection conn;
    private int TTL = ConstantUtil.getPhoenixTTL();
    private int SALT_BUCKETS = ConstantUtil.getPhoenixBUCKETS();

    public PhoenixDAO() {
        properties.setProperty("phoenix.schema.mapSystemTablesToNamespace", "true");
        properties.setProperty("phoenix.schema.isNamespaceMappingEnabled", "true");
        this.conn = new DBConnection().getConnection(dbType, phoenixURL, properties);
    }

    @Override
    public <T> void create(String tablename, Class<T> clazz) {
        HashMap<String, String> colAndType = ReflectionUtil.getColAndType(clazz);
        HashMap<String, String> colAndFamily = ReflectionUtil.getColAndFamily(clazz);
        HashMap<String, String> typeMap = PhoenixService.getTypeMap();
        String[] pkArray = ReflectionUtil.getPrimaryKey(clazz);
        String primaryKey = Stream.of(pkArray).collect(Collectors.joining(","));
        String field = colAndType.entrySet().stream().map(x -> {
                if (PhoenixService.isPrimaryKey(pkArray, x.getKey())) {
                    // 主键就不加列簇名，设置NOT NULL约束。
                    return x.getKey() + " " + typeMap.get(x.getValue()) + " NOT NULL";
                } else {
                    // 不是主键加上列簇名
                    return colAndFamily.get(x.getKey()) + "." + x.getKey() + " " + typeMap.get(x.getValue());
                }
            }
        ).collect(Collectors.joining(","));
        String sql = "CREATE TABLE IF NOT EXISTS " + tablename + " (" + field + " CONSTRAINT PK PRIMARY KEY(" + primaryKey + ")) SALT_BUCKETS=" + SALT_BUCKETS + ", TTL=" + TTL;
        System.out.println(sql);
        try {
            Statement stmt = this.conn.createStatement();
            stmt.execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public <T> ArrayList<T> select(String tablename, Class<T> clazz, String[] cols, ArrayList<Tuple3<String, String, String>> conds) {
        return SQLUtil.select(this.conn, tablename, clazz, cols, conds);
    }

    @Override
    public <T> void insert(String tablename, Class<T> clazz, ArrayList<BaseRecord> records, boolean ifIgnoreDuplicateKey) {
        if (records.size() == 0) {
            System.out.println("record is null !!");
            return;
        }
        String[] cols = ReflectionUtil.getCols(clazz);
        HashMap<String, String> colAndType = ReflectionUtil.getColAndType(clazz);
        String fields = Stream.of(cols).collect(Collectors.joining(","));
        String valueNUM = StringUtils.repeat("?,", cols.length);
        String sql = "UPSERT INTO " + tablename + " (" + fields + ") VALUES(" + valueNUM.substring(0, valueNUM.length() - 1) + ")";

        sql = ifIgnoreDuplicateKey ? sql + " ON DUPLICATE KEY IGNORE" : sql;
        System.out.println(sql);
        try {
            this.conn.setAutoCommit(false);
            PreparedStatement pstmt = this.conn.prepareStatement(sql);
            for (int i = 0; i < records.size(); i++) {
                HashMap<String, String> colAndValue = ReflectionUtil.getColAndValue(records.get(i), clazz);
                for (int j = 0; j < cols.length; j++) {
                    MysqlService.setPSTMT(pstmt, j + 1, colAndType.get(cols[j]), colAndValue.get(cols[j]));
                }
                pstmt.addBatch();
            }
            pstmt.executeBatch();
            this.conn.commit();
            this.conn.setAutoCommit(true);
            pstmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    @Override
    public <T> int update(String tablename, Class<T> clazz, ArrayList<Tuple2<String, String>> cols, ArrayList<Tuple3<String, String, String>> conds) {
        return 0;
    }

    @Override
    public <T> int delete(String tablename, Class<T> clazz, ArrayList<Tuple3<String, String, String>> conds) {
        return SQLUtil.delete(this.conn, tablename, clazz, conds);
    }
}
