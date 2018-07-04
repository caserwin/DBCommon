package jdbc.app;

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
        String table = "company";

        MysqlDAO mysqlDAO = new MysqlDAO();
//        ArrayList<PersonRecord> records = new ArrayList<>();
//        records.add(new PersonRecord().buildFields("2", "erwin1", "19", "male"));
//        records.add(new PersonRecord().buildFields("3", "erwin2", "29", "male"));
//        records.add(new PersonRecord().buildFields("4", "erwin3", "25", "female"));

        ArrayList<CompanyRecord> records = new ArrayList<>();
        records.add(new CompanyRecord().buildFields("2", "company1", "杭州", "111"));
        records.add(new CompanyRecord().buildFields("3", "company2", "上海", "222"));
        records.add(new CompanyRecord().buildFields("4", "company3", "北京", "333"));

        // 创建表
        mysqlDAO.create(table, CompanyRecord.class);
        // 插入表
//        mysqlDAO.insert(table, CompanyRecord.class, records);
        // 查询表
        ArrayList<Tuple3<String, String, String>> conds = new ArrayList<>();
        conds.add(new Tuple3("cname", "like", "compa%"));

        ArrayList<CompanyRecord> companyRecords = mysqlDAO.select(table, CompanyRecord.class, new String[]{"cid", "cname"}, conds);
        for (CompanyRecord cr : companyRecords) {
            System.out.println(cr);
        }
    }
}
