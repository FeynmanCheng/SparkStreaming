import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.lib.PageRank;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.reflect.ClassTag;

import java.util.LinkedList;
import java.util.List;

public class Neighbour {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("spark://master:7077").setAppName("HotCount");
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> file = context.textFile("hdfs://172.19.241.159:9000/user/hadoop/neighbours.json");
        JavaRDD<List<Room>> hosts = file.map(f-> {
            String[] jsons = f.split(System.getProperty("line.separator"));
            List<Room> res = new LinkedList<>();
            for (String json : jsons) {
                JSONObject object = JSON.parseObject(json);
                Room room = new Room();
                room.hostName = object.getString("host");
                room.rid = object.getString("rid");
                room.neighbours = object.getJSONArray("list").toJavaList(HostRidPair.class);
                res.add(room);
            }
            return res;
        });

        JavaRDD<Edge<String>> edge = null;
        JavaRDD<Tuple2<Object,String>> vertexes = null;



        ClassTag<String> stringClassTag = ClassTag.apply(String.class);
        Graph<String,String> graph = Graph.apply(vertexes.rdd(),edge.rdd(),"", StorageLevel.MEMORY_ONLY(),StorageLevel.MEMORY_ONLY(),stringClassTag,stringClassTag);
        PageRank.run(graph,1,0.01,stringClassTag,stringClassTag);


    }
}
