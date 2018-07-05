package jdbc.common;

import jdbc.common.tuple.Tuple3;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by yidxue on 2018/7/4
 */
public class SQLUtil {

    public static <T> ArrayList<T> select(Connection conn, String tablename, Class<T> clazz, String[] cols, ArrayList<Tuple3<String, String, String>> conds) {
        ArrayList<T> recordLS = new ArrayList<>();
        String fields = Stream.of(cols).collect(Collectors.joining(","));
        HashMap<String, String> colAndType = ReflectionUtil.getColAndType(clazz);
        String sql = "SELECT " + fields + " FROM " + tablename;
        if (conds != null && conds.size() != 0) {
            String condStr = conds.stream().map(x -> {
                if ("string".equals(colAndType.get(x.column.toLowerCase()).toLowerCase())) {
                    return x.column + " " + x.operator + " " + getStrInCheck(x.operator, x.value);
                } else {
                    return x.column + " " + x.operator + " " + x.value;
                }
            }).collect(Collectors.joining(" and "));
            sql = sql + " where " + condStr;
        }
        System.out.println(sql);
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                LinkedHashMap<String, String> colAndValue = new LinkedHashMap<>();
                for (String col : cols) {
                    colAndValue.put(col, rs.getString(col));
                }
                recordLS.add(ReflectionUtil.buildFields(clazz, colAndValue));
            }
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return recordLS;
    }

    private static String getStrInCheck(String operator, String value) {
        if ("in".equals(operator.toLowerCase())) {
            return value;
        } else {
            return "'" + value + "'";
        }
    }
}
