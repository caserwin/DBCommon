package jdbc.app;

import jdbc.app.record.PersonRecord;
import jdbc.common.BaseRecord;
import jdbc.common.tuple.Tuple2;
import jdbc.common.tuple.Tuple3;
import jdbc.mysql.MysqlDAO;

import java.util.ArrayList;

/**
 * Created by yidxue on 2018/7/4
 */
public class MySQLTestAPP {
    public static void main(String[] args) {
        String table = "person";
        MysqlDAO mysqlDAO = new MysqlDAO();

        // 创建表
//        mysqlDAO.create(table, PersonRecord.class);
        // 插入表
//        ArrayList<BaseRecord> records = new ArrayList<>();
//        records.add(new PersonRecord().buildFields("2", "erwin1", "19", "male"));
//        records.add(new PersonRecord().buildFields("3", "erwin2", "29", "male"));
//        records.add(new PersonRecord().buildFields("4", "erwin3", "25", "female"));
//        mysqlDAO.insert(table, PersonRecord.class, records);
        // 查询表
//        ArrayList<Tuple3<String, String, String>> conds = new ArrayList<>();
//        conds.add(new Tuple3("name", "like", "erwin%"));
//
//        ArrayList<PersonRecord> personRecords = mysqlDAO.select(table, PersonRecord.class, new String[]{"id", "name"}, conds);
//        for (PersonRecord cr : personRecords) {
//            System.out.println(cr);
//        }
        // 更新表
        ArrayList<Tuple2<String, String>> cols = new ArrayList<>();
        cols.add(new Tuple2("name", "caroline"));
        cols.add(new Tuple2("age", "26"));

        ArrayList<Tuple3<String, String, String>> conds = new ArrayList<>();
        conds.add(new Tuple3("gender", "=", "female"));
        int affectNum = mysqlDAO.update(table, PersonRecord.class, cols, conds);
        System.out.println("更新" + affectNum + "行！");
    }
}
