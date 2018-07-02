package jdbc.phoenix;

import jdbc.bean.PersonRecord;
import jdbc.conn.DBConnection;
import jdbc.service.ReflectionService;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.stream.Collectors;

/**
 * Created by yidxue on 2018/6/28
 * https://my.vertica.com/docs/8.1.x/HTML/index.htm#Authoring/ConnectingToVertica/ClientJDBC/BatchInsertsUsingJDBCPreparedStatements.htm
 */
public class PhoenixDAO {

    private String URLPHOENIX = "jdbc:hive2://localhost:10001/default";
    private String DBType = "phoenix";
    private Connection conn;
    private int TTL = 3600*24*365;
    private int SALT_BUCKETS = 50;

    public PhoenixDAO() {
//        this.conn = DBConnection.getConnection(DBType, URLPHOENIX);

    }



    public <T> void create(String tablename, Class<T> clazz) {
        HashMap<String, String> colAndType = ReflectionService.getColAndType(clazz);
        HashMap<String, String> typeMap = PhoenixService.getTypeMap();
        String[] String = ReflectionService.getPrimaryKey(clazz);
        String field = colAndType.entrySet().stream().map(x -> x.getKey() + " " + x.getValue()).collect(Collectors.joining(","));
        String sql = "CREATE TABLE IF NOT EXISTS " + tablename +" "+field+" SALT_BUCKETS="+SALT_BUCKETS+" TTL=" + TTL;
        System.out.println(sql);

//        try {
//            HashMap<String, String> colAndType = ReflectionService.getColAndType(clazz);
//            String fields = colAndType.entrySet().stream().map(x -> x.getKey() + "\t" + x.getValue()).collect(Collectors.joining(","));
//            Statement stmt = this.conn.createStatement();
//            String sql = "create table if not exists " + tablename + "(" + fields + ")"
//                             + " row format delimited fields terminated by '\\t'"
//                             + " collection items terminated by ','";
//
//            System.out.println(sql);
//            stmt.execute(sql);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
    }

    public static void main(String[] args){
        PhoenixDAO phoenixDAO =new PhoenixDAO();
        phoenixDAO.create("ttt", PersonRecord.class);


    }



    /**
     * insert into table
     */
    public <T> void insert(ArrayList<T> records, Class<T> clazz, String tableName) {
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
}
