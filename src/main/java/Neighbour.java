import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.lib.PageRank;
import org.apache.spark.storage.StorageLevel;
import org.bson.Document;
import scala.Tuple2;
import scala.reflect.ClassTag;

import java.util.LinkedList;
import java.util.List;

public class Neighbour {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("spark://master:7077").setAppName("HotCount");
        JavaSparkContext context = new JavaSparkContext(conf);
        MongoClient client = new MongoClient("172.19.241.171");
        MongoDatabase database = client.getDatabase("general");
        MongoCollection<Document> collection = database.getCollection("host");
        collection.find(); //TODO input from mongoDB

        JavaRDD<String> file = context.textFile("hdfs://172.19.241.159:9000/user/hadoop/neighbours.json");

        String fileContent = file.collect().get(0);
        String[] jsons = fileContent.split(System.getProperty("line.separator"));
        List<Room> roomList = new LinkedList<>();
        for (String json : jsons) {
            JSONObject object = JSON.parseObject(json);
            Room room = new Room();
            room.hostName = object.getString("host");
            room.rid = object.getLong("rid");
            room.neighbours = object.getJSONArray("list").toJavaList(HostRidPair.class);
            roomList.add(room);
        }
        JavaRDD<Room> roomRDDs = context.parallelize(roomList);

        List<Edge<String>> edges = new LinkedList<>();

        for(Room room: roomList){
            List<HostRidPair> hostRidPairs = room.neighbours;
            for (HostRidPair pair: hostRidPairs){
                Edge<String> ed = new Edge<>(pair.rid,room.rid,pair.hostName);
                edges.add(ed);
            }
        }

        JavaRDD<Edge<String>> edge = context.parallelize(edges);
        JavaRDD<Tuple2<Object,String>> vertexes = roomRDDs.map(room-> new Tuple2<>(room.rid, room.hostName));



        ClassTag<String> stringClassTag = ClassTag.apply(String.class);
        Graph<String,String> graph = Graph.apply(vertexes.rdd(),edge.rdd(),"", StorageLevel.MEMORY_ONLY(),StorageLevel.MEMORY_ONLY(),stringClassTag,stringClassTag);
        PageRank.run(graph,10,0.0001,stringClassTag,stringClassTag);


    }
}
