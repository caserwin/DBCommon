package jdbc.app;

import jdbc.app.record.PersonRecord;
import jdbc.common.BaseRecord;
import jdbc.phoenix.PhoenixDAO;

import java.util.ArrayList;

/**
 * Created by yidxue on 2018/7/4
 */
public class PhoenixTestAPP {
    public static void main(String[] args){
        String table = "person";

        ArrayList<BaseRecord> records = new ArrayList<>();
        records.add(new PersonRecord().buildFields("2", "erwin1", "19", "male"));
        records.add(new PersonRecord().buildFields("3", "erwin2", "29", "male"));
        records.add(new PersonRecord().buildFields("4", "erwin3", "25", "female"));

        PhoenixDAO phoenixDAO = new PhoenixDAO();
        // 创建表
//        phoenixDAO.create("person", PersonRecord.class);
        // 插入表
        phoenixDAO.insert("person", PersonRecord.class, records, true);
//        // 查询表
//        ArrayList<Tuple3<String, String, String>> conds = new ArrayList<>();
//        conds.add(new Tuple3("name", "=", "erwin1"));
//        conds.add(new Tuple3("id", "=", "1"));
//
//        ArrayList<PersonRecord> personRecords = mysqlDAO.select("person", PersonRecord.class, new String[]{"id", "name"}, conds);
//        for (PersonRecord ps : personRecords) {
//            System.out.println(ps);
//        }
    }
}
