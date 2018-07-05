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
//        if (args.length != 1) {
//            System.out.println("args error!");
//            return;
//        }
        String table = "person";

        MysqlDAO mysqlDAO = new MysqlDAO();
        ArrayList<BaseRecord> records = new ArrayList<>();
        records.add(new PersonRecord().buildFields("2", "erwin1", "19", "male"));
        records.add(new PersonRecord().buildFields("3", "erwin2", "29", "male"));
        records.add(new PersonRecord().buildFields("4", "erwin3", "25", "female"));

//        // 创建表
//        mysqlDAO.create(table, PersonRecord.class);
//        // 插入表
//        mysqlDAO.insert(table, PersonRecord.class, records);
//        // 查询表
//        ArrayList<Tuple3<String, String, String>> conds = new ArrayList<>();
//        conds.add(new Tuple3("name", "like", "erwin%"));
//
//        ArrayList<PersonRecord> personRecords = mysqlDAO.select(table, PersonRecord.class, new String[]{"id", "name"}, conds);
//        for (PersonRecord cr : personRecords) {
//            System.out.println(cr);
//        }
        // 更新表
        ArrayList<Tuple2<String, String>> cols = new ArrayList<>();
        cols.add(new Tuple2("name", "erwin"));
        cols.add(new Tuple2("age", "25"));
        ArrayList<Tuple3<String, String, String>> conds2 = new ArrayList<>();
        conds2.add(new Tuple3("name", "like", "222%"));
        mysqlDAO.update(table, PersonRecord.class, cols, conds2);

    }
}
