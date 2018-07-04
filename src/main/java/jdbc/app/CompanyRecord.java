package jdbc.app;

import jdbc.common.BaseRecord;
import java.util.LinkedHashMap;

/**
 * Created by yidxue on 2018/7/4
 */
public class CompanyRecord extends BaseRecord{

    public static String cls = "company";

    @Override
    public CompanyRecord buildFields(String... args) {
        super.setColAndValue("cid", args[0]);
        super.setColAndValue("cname", args[1]);
        super.setColAndValue("addr", args[2]);
        super.setColAndValue("number", args[3]);
        return this;
    }

    @Override
    public CompanyRecord buildFields(LinkedHashMap<String, String> colAndValue) {
        super.setColAndValue(colAndValue);
        return this;
    }
}
