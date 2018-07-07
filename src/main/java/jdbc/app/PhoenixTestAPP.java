package jdbc.app;

import jdbc.app.record.PersonRecord;
import jdbc.common.BaseRecord;
import jdbc.common.tuple.Tuple2;
import jdbc.common.tuple.Tuple3;
import jdbc.phoenix.PhoenixDAO;
import java.util.ArrayList;

/**
 * Created by yidxue on 2018/7/4
 */
public class PhoenixTestAPP {
    public static void main(String[] args) {
        String table = "person";

        ArrayList<BaseRecord> records = new ArrayList<>();
        records.add(new PersonRecord().buildFields("2", "erwin1", "19", "male"));
        records.add(new PersonRecord().buildFields("3", "caroline", "28", "female"));
        records.add(new PersonRecord().buildFields("4", "erwin3", "25", "male"));

        PhoenixDAO phoenixDAO = new PhoenixDAO();
        // 创建表
        phoenixDAO.create(table, PersonRecord.class);
        // 插入表
        phoenixDAO.insert(table, PersonRecord.class, records, true);
        // 查询表
        ArrayList<Tuple3<String, String, String>> conds = new ArrayList<>();
        conds.add(new Tuple3("name", "=", "erwin1"));
        conds.add(new Tuple3("id", "in", "(1,2,3)"));

        ArrayList<PersonRecord> personRecords = phoenixDAO.select(table, PersonRecord.class, new String[]{"id", "name"}, conds);
        for (PersonRecord ps : personRecords) {
            System.out.println(ps);
        }
        // 更新行
        ArrayList<Tuple2<String, String>> cols = new ArrayList<>();
        cols.add(new Tuple2("age", "18"));

        ArrayList<Tuple3<String, String, String>> conds2 = new ArrayList<>();
        conds2.add(new Tuple3("gender", "=", "female"));
        conds2.add(new Tuple3("name", "=", "caroline"));
        phoenixDAO.update(table, PersonRecord.class, cols, conds2);

        // 删除行
        ArrayList<Tuple3<String, String, String>> conds3 = new ArrayList<>();
        conds3.add(new Tuple3("name", "=", "erwin3"));
        int affectNum2 = phoenixDAO.delete(table, PersonRecord.class, conds3);
        System.out.println("删除" + affectNum2 + "行！");
    }
}
