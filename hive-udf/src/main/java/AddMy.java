import org.apache.hadoop.hive.ql.exec.UDF;

public class AddMy extends UDF {
    /**
     * 两参数 求和
     * @param data1
     * @param data2
     * @return
     */
    public int evaluate (int data1,int data2) {
        return data1+data2;
    }

    /**
     * 单参数+1
     * @param data1
     * @return
     */
    public int evaluate (int data1) {
        return data1+1;
    }
}
