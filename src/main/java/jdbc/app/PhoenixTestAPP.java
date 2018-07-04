package jdbc.app;

/**
 * Created by yidxue on 2018/7/4
 */
public class PhoenixTestAPP {
    public static void main(String[] args){
//        if (args.length != 2) {
//            System.out.println("args error!");
//            return;
//        }
//        String table = args[0];
//        String path = args[1];
//
//        HiveDAO hiveDAO = new HiveDAO();
//        ArrayList<PersonRecord> records = new ArrayList<>();
//        records.add(new PersonRecord().buildFields("2", "erwin1", "19", "male"));
//        records.add(new PersonRecord().buildFields("3", "erwin2", "29", "male"));
//        records.add(new PersonRecord().buildFields("4", "erwin3", "25", "female"));
//
//        MysqlDAO mysqlDAO = new MysqlDAO();
//        // 创建表
//        mysqlDAO.create("person", PersonRecord.class);
//        // 插入表
//        mysqlDAO.insert("person", PersonRecord.class, records);
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
