package jdbc.mysql;

import jdbc.conn.DBConnection;
import org.apache.commons.lang.StringUtils;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by yidxue on 2018/6/30
 */
public class MysqlDAO {

    private String URLMYSQL = "jdbc:mysql://xxx:3306/dbname";
    private String username = "";
    private String password = "";
    private String DBType = "mysql";
    private Connection conn;

    public MysqlDAO() {
        this.conn = DBConnection.getConnection(DBType, URLMYSQL, username, password);
    }

    public <T> void insert(ArrayList<T> records, Class<T> clazz, String tableName) {
        if (records.size() == 0) {
            System.out.println("record is null !!");
            return;
        }

        try {
            String[] cols = (String[]) clazz.getMethod("getAttributes").invoke(null);
            String fields = Stream.of(cols).map(x -> x.split("\\s+")[0]).collect(Collectors.joining(","));
            String valueNUM = StringUtils.repeat("?,", cols.length);
            String sql = "INSERT INTO " + tableName + " (" + fields + ") VALUES(" + valueNUM.substring(0, valueNUM.length() - 1) + ")";

            conn.setAutoCommit(false);
            PreparedStatement pstmt = conn.prepareStatement(sql);

            for (int i = 0; i < records.size(); i++) {
                for (int j = 0; j < cols.length; j++) {

                }
                pstmt.addBatch();
            }

//            for (StudentClsHour record : records) {
//                pstmt.set(1, record.getSid());
//                pstmt.setInt(2, record.getCls_type());
//                pstmt.setInt(3, record.getCls_hour());
//                pstmt.setInt(4, record.getCreate_time());
//                pstmt.setInt(5, record.getExpire_time());
//                pstmt.setInt(6, record.getFor_free());
//                pstmt.setInt(7, record.getOrder_id());
//                pstmt.setInt(8, record.getUse_level());
//                pstmt.setInt(9, record.getCls_hour_bak());
//                pstmt.setInt(10, record.getCls_type_bak());
//                pstmt.setInt(11, record.getExpire_time_bak());
//
//                pstmt.addBatch();
//            }

            pstmt.executeBatch();
            conn.commit();
            conn.close();

        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            System.out.println("get attribute error !!");
            e.printStackTrace();
        } catch (SQLException e) {
            System.out.println("get sql error !!");
            e.printStackTrace();
        }
    }


//    @Override
//    public void insert(String tablename, ArrayList records) {
//        if (records.size() == 0) {
//            System.out.println("record is null !!");
//            return;
//        }
//        Connection conn = DBConnection.getConnection(DBType, url, username, password);
//        try {
//            String[] cols = (String[]) records.get(0).getClass().getMethod("getAttributes").invoke(null);
//            String fields = Stream.of(cols).map(x -> x.split("\\s+")[0]).collect(Collectors.joining(","));
//            String valueNUM = StringUtils.repeat("?,", cols.length);
//            String sql = "INSERT INTO " + tablename + " (" + fields + ") VALUES(" + valueNUM.substring(0, valueNUM.length() - 1) + ")";
//
//            conn.setAutoCommit(false);
//            PreparedStatement pstmt = conn.prepareStatement(sql);
//
//            for (int i = 0; i <records.size() ; i++) {
//                for (int j = 0; j < cols.length; j++) {
//
//                }
//                pstmt.addBatch();
//            }
//
//            for (StudentClsHour record : records) {
//                pstmt.set(1, record.getSid());
//                pstmt.setInt(2, record.getCls_type());
//                pstmt.setInt(3, record.getCls_hour());
//                pstmt.setInt(4, record.getCreate_time());
//                pstmt.setInt(5, record.getExpire_time());
//                pstmt.setInt(6, record.getFor_free());
//                pstmt.setInt(7, record.getOrder_id());
//                pstmt.setInt(8, record.getUse_level());
//                pstmt.setInt(9, record.getCls_hour_bak());
//                pstmt.setInt(10, record.getCls_type_bak());
//                pstmt.setInt(11, record.getExpire_time_bak());
//
//                pstmt.addBatch();
//            }
//
//            pstmt.executeBatch();
//            conn.commit();
//            conn.close();
//
//        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
//            System.out.println("get attribute error !!");
//            e.printStackTrace();
//        }catch (SQLException e) {
//            System.out.println("get sql error !!");
//            e.printStackTrace();
//        }
//    }


//    public static void main(String[] args) {
//        MysqlDao mysqlDao = new MysqlDao();
//        ArrayList<PersonRecord> ls = new ArrayList<>();
//        ls.add(new PersonRecord(1, "erwin", 3, "male"));
//        mysqlDao.insert("tb_Name", ls);
//
//    }
}