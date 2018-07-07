package jdbc.app;

import jdbc.app.record.PersonRecord;
import jdbc.common.BaseRecord;
import jdbc.common.tuple.Tuple3;
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

        // 删除行
        ArrayList<Tuple3<String, String, String>> conds3 = new ArrayList<>();
        conds3.add(new Tuple3("name", "=", "erwin2"));
        int affectNum2 = phoenixDAO.delete(table, PersonRecord.class, conds3);
        System.out.println("更新" + affectNum2 + "行！");
    }
}
