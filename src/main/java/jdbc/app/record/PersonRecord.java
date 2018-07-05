package jdbc.app.record;

import jdbc.common.BaseRecord;
import java.util.LinkedHashMap;

/**
 * Created by yidxue on 2018/6/30
 */
public class PersonRecord extends BaseRecord {

    public static String cls = "person";

    @Override
    public BaseRecord buildFields(String... args) {
        super.setColAndValue("id", args[0]);
        super.setColAndValue("name", args[1]);
        super.setColAndValue("age", args[2]);
        super.setColAndValue("gender", args[3]);
        return this;
    }

    @Override
    public BaseRecord buildFields(LinkedHashMap<String, String> colAndValue) {
        super.setColAndValue(colAndValue);
        return this;
    }
}