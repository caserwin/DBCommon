package jdbc.mysql;

import jdbc.common.BaseRecord;
import jdbc.common.DBOperate;
import jdbc.common.ReflectionUtil;
import jdbc.common.SQLUtil;
import jdbc.common.tuple.Tuple2;
import jdbc.common.tuple.Tuple3;
import jdbc.common.conn.DBConnection;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by yidxue on 2018/6/30
 * https://my.vertica.com/docs/8.1.x/HTML/index.htm#Authoring/ConnectingToVertica/ClientJDBC/BatchInsertsUsingJDBCPreparedStatements.htm
 */
public class MysqlDAO implements DBOperate<Object> {

    private String URLMYSQL = "jdbc:mysql://localhost:3306/test";
    private String username = "root";
    private String password = "";
    private String DBType = "mysql";
    private Connection conn;

    public MysqlDAO() {
        this.conn = new DBConnection().getConnection(DBType, URLMYSQL, username, password);
    }

    @Override
    public <T> void create(String tablename, Class<T> clazz) {
        HashMap<String, String> colAndType = ReflectionUtil.getColAndType(clazz);
        HashMap<String, String> colAndComment = ReflectionUtil.getColAndComment(clazz);

        HashMap<String, String> typeMap = MysqlService.getTypeMap();
        String[] pkArray = ReflectionUtil.getPrimaryKey(clazz);
        String fields = colAndType.entrySet().stream().map(x -> {
            if (MysqlService.isPrimaryKey(pkArray, x.getKey())) {
                if (colAndComment.size() == 0) {
                    return "`" + x.getKey() + "` " + typeMap.get(x.getValue()) + " NOT NULL";
                } else {
                    return "`" + x.getKey() + "` " + typeMap.get(x.getValue()) + " NOT NULL COMMENT '" + colAndComment.get(x.getKey()) + "'";
                }
            } else {
                if (colAndComment.size() == 0) {
                    return "`" + x.getKey() + "` " + typeMap.get(x.getValue()) + " NOT NULL";
                } else {
                    return "`" + x.getKey() + "` " + typeMap.get(x.getValue()) + " NOT NULL COMMENT '" + colAndComment.get(x.getKey()) + "'";
                }
            }
        }).collect(Collectors.joining(","));

        String primaryKey = Stream.of(pkArray).map(x -> "`" + x + "`").collect(Collectors.joining(","));
        String sql = "CREATE TABLE IF NOT EXISTS `" + tablename + "` (" + fields + ", PRIMARY KEY (" + primaryKey + ")) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;";
        System.out.println(sql);
        try {
            Statement stmt = this.conn.createStatement();
            stmt.execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public <T> void insert(String tablename, Class<T> clazz, ArrayList<BaseRecord> records) {
        if (records.size() == 0) {
            System.out.println("record is null !!");
            return;
        }
        String[] cols = ReflectionUtil.getCols(clazz);
        HashMap<String, String> colAndType = ReflectionUtil.getColAndType(clazz);

        String fields = Stream.of(cols).map(x -> "`" + x + "`").collect(Collectors.joining(","));
        String valueNUM = StringUtils.repeat("?,", cols.length);
        String sql = "INSERT INTO `" + tablename + "` (" + fields + ") VALUES(" + valueNUM.substring(0, valueNUM.length() - 1) + ");";
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
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public <T> ArrayList<T> select(String tablename, Class<T> clazz, String[] cols, ArrayList<Tuple3<String, String, String>> conds) {
        return SQLUtil.select(this.conn, tablename, clazz, cols, conds);
    }

    @Override
    public <T> void update(String tablename, Class<T> clazz, ArrayList<Tuple2<String, String>> cols, ArrayList<Tuple3<String, String, String>> conds) {
        SQLUtil.update(this.conn, tablename, clazz, cols, conds);
    }
}