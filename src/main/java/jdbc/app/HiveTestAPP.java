package jdbc.app;

import jdbc.app.record.PersonRecord;
import jdbc.common.BaseRecord;
import jdbc.common.tuple.Tuple3;
import jdbc.hive.HiveDAO;
import java.util.ArrayList;

/**
 * Created by yidxue on 2018/7/4
 */
public class HiveTestAPP {
    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("args error!");
            return;
        }
        String table = args[0];
        String path = args[1];

        HiveDAO hiveDAO = new HiveDAO();
        ArrayList<BaseRecord> records = new ArrayList<>();
        records.add(new PersonRecord().buildFields("2", "erwin1", "19", "male"));
        records.add(new PersonRecord().buildFields("3", "erwin2", "29", "male"));
        records.add(new PersonRecord().buildFields("4", "erwin3", "25", "female"));

        // 创建表
        hiveDAO.create(table, PersonRecord.class);
        // 插入表
        hiveDAO.loadToHive(records, PersonRecord.class, table, path);
        // 查询表
        ArrayList<Tuple3<String, String, String>> conds = new ArrayList<>();
        conds.add(new Tuple3("name", "like", "erwin%"));
        conds.add(new Tuple3("id", "in", "(1,2)"));

        ArrayList<PersonRecord> personRecords = hiveDAO.select(table, PersonRecord.class, new String[]{"id", "name"}, conds);
        for (PersonRecord ps : personRecords) {
            System.out.println(ps);
        }
    }
}
