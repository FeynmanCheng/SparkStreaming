import scala.Serializable;
import scala.Tuple2;


public class CateHostHot implements Serializable,Comparable<CateHostHot>{
    Tuple2<String,Integer> t;

    public CateHostHot(Tuple2<String,Integer> t){
        this.t = t;
    }


    @Override
    public int compareTo(CateHostHot o) {
        return o.t._2-t._2;
    }
}
