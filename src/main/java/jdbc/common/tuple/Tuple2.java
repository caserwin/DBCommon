package jdbc.common.tuple;

/**
 * Created by yidxue on 2018/7/3
 */
public class Tuple2<A, B> {
    public final A col;
    public final B value;

    public Tuple2(A col, B value) {
        this.col = col;
        this.value = value;
    }
}
