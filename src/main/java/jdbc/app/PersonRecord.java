package jdbc.app;

import jdbc.common.BaseRecord;
import jdbc.common.tuple.Tuple3;
import jdbc.hive.HiveDAO;
import jdbc.mysql.MysqlDAO;

import java.util.ArrayList;
import java.util.LinkedHashMap;

/**
 * Created by yidxue on 2018/6/30
 */
public class PersonRecord extends BaseRecord {

    public static String cls = "person";

    @Override
    public PersonRecord buildFields(String... args) {
        super.setColAndValue("id", args[0]);
        super.setColAndValue("name", args[1]);
        super.setColAndValue("age", args[2]);
        super.setColAndValue("gender", args[3]);
        return this;
    }

    @Override
    public PersonRecord buildFields(LinkedHashMap<String, String> colAndValueMap) {
        super.setColAndValue(colAndValueMap);
        return this;
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("args error!");
            return;
        }
        String table = args[0];
        String path = args[1];

        HiveDAO hiveDAO = new HiveDAO();
        ArrayList<PersonRecord> records = new ArrayList<>();
        records.add(new PersonRecord().buildFields("1", "erwin1", "19", "male"));
        records.add(new PersonRecord().buildFields("2", "erwin2", "29", "male"));
        records.add(new PersonRecord().buildFields("3", "erwin3", "25", "female"));

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


        MysqlDAO mysqlDAO = new MysqlDAO();
        // 创建表
        mysqlDAO.create("table1", PersonRecord.class);
        // 插入表
        mysqlDAO.insert("table1", PersonRecord.class, records);
        // 查询表

    }
}