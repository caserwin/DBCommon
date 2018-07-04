package jdbc.mysql;

import jdbc.app.PersonRecord;
import jdbc.common.DBOperate;
import jdbc.common.ReflectionUtil;
import jdbc.common.tuple.Tuple2;
import jdbc.common.tuple.Tuple3;
import jdbc.conn.DBConnection;
import org.apache.commons.lang.StringUtils;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by yidxue on 2018/6/30
 */
public class MysqlDAO implements DBOperate<Object> {

    private String URLMYSQL = "jdbc:mysql://localhost:3306/test";
    private String username = "root";
    private String password = "";
    private String DBType = "mysql";
    private Connection conn;

    public MysqlDAO() {
        this.conn = DBConnection.getConnection(DBType, URLMYSQL, username, password);
    }


    public static void main(String[] args) {
        MysqlDAO mysqlDAO = new MysqlDAO();
//        mysqlDAO.create("table", PersonRecord.class);
        ArrayList<PersonRecord> records = new ArrayList<>();
        records.add(new PersonRecord().buildFields("1", "erwin1", "19", "male"));
        records.add(new PersonRecord().buildFields("2", "erwin2", "29", "male"));
        records.add(new PersonRecord().buildFields("3", "erwin3", "25", "female"));
        mysqlDAO.insert("ttt", PersonRecord.class, records);
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
        String sql = "CREATE TABLE `" + tablename + "` (" + fields + ", PRIMARY KEY (" + primaryKey + ")) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci";
        try {
            Statement stmt = this.conn.createStatement();
            stmt.execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public <T> void insert(String tablename, Class<T> clazz, ArrayList<T> records) {
        if (records.size() == 0) {
            System.out.println("record is null !!");
            return;
        }
        try {
            String[] cols = (String[]) clazz.getMethod("getCols").invoke(null);
            String fields = Stream.of(cols).collect(Collectors.joining(","));
            String valueNUM = StringUtils.repeat("?,", cols.length);
            String sql = "INSERT INTO " + tablename + " (" + fields + ") VALUES(" + valueNUM.substring(0, valueNUM.length() - 1) + ")";
            System.out.println(sql);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            System.out.println("get attribute error !!");
            e.printStackTrace();
        }
    }

    @Override
    public <T> ArrayList<T> select(String tablename, Class<T> clazz, String[] cols, ArrayList<Tuple3<String, String, String>> cond) {
        return null;
    }

    @Override
    public <T> void update(String tablename, Class<T> clazz, ArrayList<Tuple2<String, String>> cols, ArrayList<Tuple3<String, String, String>> cond) {

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