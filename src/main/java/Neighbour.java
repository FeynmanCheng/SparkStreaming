import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.lib.PageRank;
import org.apache.spark.storage.StorageLevel;
import org.bson.Document;
import scala.Tuple2;
import scala.reflect.ClassTag;

import java.util.*;

public class Neighbour {
    public static void main(String[] args) {
//        SparkConf conf = new SparkConf().setMaster("spark://master:7077").setAppName("HotCount");
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("HotCount");
        JavaSparkContext context = new JavaSparkContext(conf);
        MongoClient client = new MongoClient("172.19.241.171");
        MongoDatabase database = client.getDatabase("general");
        MongoCollection<Document> collection = database.getCollection("host");
        MongoCursor<Document> cursor = collection.find().iterator();

        List<Room> roomList = new LinkedList<>();
        while (cursor.hasNext()){
            Document document = cursor.next();
            Room room = new Room();
            room.neighbours = new LinkedList<>();
            room.rid = Integer.toUnsignedLong(document.getInteger("rid"));
//            room.hostName = document.getString("name");
            List<Document> documents = document.getList("anchorFriends", Document.class);
            if (documents != null && documents.size() != 0){
                for (Document d: documents){
                    AnchorFriend a = new AnchorFriend();
                    a.isMutual = d.getBoolean("isMutual");
                    a.avatar = d.getString("avatar");
                    a.name = d.getString("name");
                    a.rid = d.getInteger("rid");
                    room.neighbours.add(a);
                }
            }
            roomList.add(room);
        }

        JavaRDD<Room> roomRDDs = context.parallelize(roomList);
        List<Edge<String>> edgeList = new LinkedList<>();
        Map<Long,String> inDegree  = new HashMap<>();
        for(Room room: roomList){
            List<AnchorFriend> anchorFriends = room.neighbours;
            for (AnchorFriend pair: anchorFriends){
                Edge<String> ed = new Edge<>(room.rid,pair.rid,pair.name);
                edgeList.add(ed);
                if (!inDegree.containsKey(pair.rid)){
                    inDegree.put(pair.rid,pair.name);
                }
            }
        }

        Broadcast<Map<Long,String>> broadcast = context.broadcast(inDegree);

        JavaRDD<Edge<String>> edges = context.parallelize(edgeList);
        JavaRDD<Tuple2<Object,String>> vertices = roomRDDs.map(room-> {
            Map<Long,String> map = broadcast.getValue();
            if (map.containsKey(room.rid)) {
                room.hostName = inDegree.get(room.rid);

            }
            return new Tuple2<>(room.rid, room.hostName);
        });
//        vertices = vertices.filter(Objects::nonNull);
//        edges.filter(e->{
//            return broadcast.getValue().containsKey(e.dstId());
//        });
        System.out.println("Edge num: " + edges.collect().size());
        System.out.println("Vertex num: " + vertices.collect().size());
//        vertices.collect().forEach(System.out::println);
//        edges.collect().forEach(System.out::println);
        ClassTag<String> stringClassTag = ClassTag.apply(String.class);
        Graph<String,String> graph = Graph.apply(vertices.rdd(),edges.rdd(),"", StorageLevel.MEMORY_ONLY(),StorageLevel.MEMORY_ONLY(),stringClassTag,stringClassTag);
        Graph<Object,Object> result = PageRank.run(graph,3,0.001,stringClassTag,stringClassTag);
//        result.vertices().saveAsTextFile("~/vertex.txt");
        List<Tuple2<Object,Object>> list = new ArrayList<>(result.vertices().toJavaRDD().collect());//返回的List是Arrays的内部类，没有排序等方法，需要新建一个List
        list.sort(new Comparator<Tuple2<Object, Object>>() {
            @Override
            public int compare(Tuple2<Object, Object> o1, Tuple2<Object, Object> o2) {
                Double d1 = (Double) o1._2;
                Double d2 = (Double) o2._2;
                return d1.compareTo(d2);
            }
        });

        list.forEach(item ->{
            System.out.print(item);
            if (inDegree.containsKey(item._1)){
                System.out.println(inDegree.get(item._1));
            }else {
                System.out.println();

            }
        });
//        result.edges().toJavaRDD().collect().forEach(System.out::println);

    }
}
