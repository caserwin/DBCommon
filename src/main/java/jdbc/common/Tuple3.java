package jdbc.common;

/**
 * Created by yidxue on 2018/7/3
 */
public class Tuple3<A, B, C> {
    public final A column;
    public final B operator;
    public final C value;

    public Tuple3(A column, B operator, C value) {
        this.column = column;
        this.operator = operator;
        this.value = value;
    }
}
