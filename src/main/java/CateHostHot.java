import scala.Serializable;
import scala.Tuple2;
import java.util.List;

public class CateHostHot implements Serializable{
//    Tuple2<String,Integer> t;
    Tuple2<String,List<Tuple2<String,Integer>>> t;
    public CateHostHot(    Tuple2<String,List<Tuple2<String,Integer>>> t){
        this.t = t;
    }

}
