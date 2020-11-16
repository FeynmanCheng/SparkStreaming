
import scala.Serializable;
import scala.Tuple2;

/**
 * 调用 rdd.top() 时若需要传入自定义比较器，会报无法序列化的错误，因此需要将rdd的item类型加上一个 wrapper
 * 实现 Serializable,Comparable 接口
 */
public class MyTuple2 implements Serializable,Comparable<MyTuple2> {
    public Tuple2<String,Integer> t;

    public MyTuple2(Tuple2 t){
        this.t = t;
    }

    @Override
    public int compareTo(MyTuple2 o) {
        return t._2 - o.t._2;
    }
}
