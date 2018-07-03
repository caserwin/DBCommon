package jdbc.phoenix;

import jdbc.common.DBOperate;
import jdbc.app.PersonRecord;
import jdbc.conn.DBConnection;
import jdbc.common.ReflectionUtil;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by yidxue on 2018/6/28
 * https://my.vertica.com/docs/8.1.x/HTML/index.htm#Authoring/ConnectingToVertica/ClientJDBC/BatchInsertsUsingJDBCPreparedStatements.htm
 */
public class PhoenixDAO implements DBOperate<Object> {

    private String URLPHOENIX = "jdbc:phoenix:localhost:2181";
    private String DBType = "phoenix";
    private Connection conn;
    private int TTL = 3600 * 24 * 365;
    private int SALT_BUCKETS = 50;
    private String COL_FAMLIY = "info";

    public PhoenixDAO() {
        this.conn = DBConnection.getConnection(DBType, URLPHOENIX);
    }

    @Override
    public <T> void create(String tablename, Class<T> clazz) {
        HashMap<String, String> colAndType = ReflectionUtil.getColAndType(clazz);
        HashMap<String, String> typeMap = PhoenixService.getTypeMap();
        String[] pkArray = ReflectionUtil.getPrimaryKey(clazz);
        String primaryKey = Stream.of(pkArray).collect(Collectors.joining(","));
        String field = colAndType.entrySet().stream().map(x -> {
                if (PhoenixService.isPrimaryKey(pkArray, x.getKey())) {
                    // 主键就不加列簇名，设置NOT NULL约束。
                    return x.getKey() + " " + typeMap.get(x.getValue()) + " NOT NULL";
                } else {
                    // 不是主键加上列簇名
                    return COL_FAMLIY + "." + x.getKey() + " " + typeMap.get(x.getValue());
                }
            }
        ).collect(Collectors.joining(","));
        String sql = "CREATE TABLE IF NOT EXISTS " + tablename + " (" + field + " CONSTRAINT PK PRIMARY KEY(" + primaryKey + ")) SALT_BUCKETS=" + SALT_BUCKETS + ", TTL=" + TTL + ";";
        System.out.println(sql);
        try {
            Statement stmt = this.conn.createStatement();
            stmt.execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public <T> void insert(String tablename, Class<T> clazz, ArrayList<Object> record) {
//        int num = cols.size();
//        try {
//            this.conn.setAutoCommit(false);
//            String fields = cols.stream().collect(Collectors.joining(","));
//            String valueNUM = StringUtils.repeat("?,", num);
//            String sql = "INSERT INTO " + tablename + "(" + fields + ") VALUES(" + valueNUM.substring(0, valueNUM.length() - 1) + ")";
//            PreparedStatement pstmt = conn.prepareStatement(sql);
//            for (String[] row : rows) {
//                for (int i = 0; i < num; i++) {
//                    pstmt.setString(i, row[i]);
//                }
//                pstmt.addBatch();
//            }
//            pstmt.executeBatch();
//            conn.commit();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
    }

    @Override
    public <T> void select(String tablename, Class<T> clazz) {

    }


    public static void main(String[] args) {
        PhoenixDAO phoenixDAO = new PhoenixDAO();
        phoenixDAO.create("ttt", PersonRecord.class);


    }
}
