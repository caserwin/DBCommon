package jdbc;

import jdbc.bean.PersonRecord;
import jdbc.hive.HiveDAO;
import java.util.ArrayList;

/**
 * Created by yidxue on 2018/6/30
 */
public class Main {

    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("args error!");
            return;
        }
        String table = args[0];
        String path = args[1];

        HiveDAO hiveDAO = new HiveDAO();
        // create table
        hiveDAO.create(table, PersonRecord.class);

        ArrayList<PersonRecord> records = new ArrayList<>();
        records.add(new PersonRecord().buildFields("1", "erwin1", "19", "male"));
        records.add(new PersonRecord().buildFields("2", "erwin2", "29", "male"));
        records.add(new PersonRecord().buildFields("3", "erwin3", "25", "female"));

        // load data
        hiveDAO.loadToHive(records, PersonRecord.class, table, path);
    }
}
