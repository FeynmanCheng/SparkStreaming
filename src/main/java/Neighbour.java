import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
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
import java.util.*;

public class Neighbour {
    public static void main(String[] args) {
//        SparkConf conf = new SparkConf().setMaster("spark://master:7077").setAppName("Graph");
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("Graph");
        JavaSparkContext context = new JavaSparkContext(conf);
        MongoClient client = new MongoClient("172.19.241.171");
        MongoDatabase database = client.getDatabase("general");
        MongoCollection<Document> hostCollection = database.getCollection("host");
        MongoCursor<Document> cursor = hostCollection.find().iterator();

        // 从数据库读取主播及友邻等信息
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
        // 构造房间（主播）信息RDD（即图中的节点）
        JavaRDD<Room> roomRDDs = context.parallelize(roomList);
        // 构造友邻关系
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
        // 构造友邻关系RDD（即图中的边）
        JavaRDD<Edge<String>> edges = context.parallelize(edgeList);
        JavaRDD<Tuple2<Object,String>> vertices = roomRDDs.map(room-> {
            if (inDegree.containsKey(room.rid)){
                return new Tuple2<>(room.rid, room.hostName);
            }
            return null;
        });
        // 过滤掉没有被其他主播当做友邻的主播
        vertices = vertices.filter(Objects::nonNull);
        edges = edges.filter(e->{
            return inDegree.containsKey(e.srcId());
        });
        System.out.println("Edge num: " + edges.collect().size());
        System.out.println("Vertex num: " + vertices.collect().size());

        // 进行 PageRank 计算
        ClassTag<String> stringClassTag = ClassTag.apply(String.class);
        Graph<String,String> graph = Graph.apply(vertices.rdd(),edges.rdd(),"", StorageLevel.MEMORY_ONLY(),StorageLevel.MEMORY_ONLY(),stringClassTag,stringClassTag);
        Graph<Object,Object> result = PageRank.run(graph,2,0.001,stringClassTag,stringClassTag);

        // 对结果进行处理，排序权值等操作
        List<Tuple2<Object,Object>> pageRankResultList = new ArrayList<>(result.vertices().toJavaRDD().collect());//返回的List是Arrays的内部类，没有排序等方法，需要新建一个List
        pageRankResultList.sort((o1, o2) -> {
            Double d1 = (Double) o1._2;
            Double d2 = (Double) o2._2;
            return d1.compareTo(d2);
        });


        // 结果输出至 mongoDB
        List<Document> verticesOutputList = new LinkedList<>();
        pageRankResultList.forEach(item ->{
            System.out.print(item+ " ");

            if (inDegree.containsKey(item._1)){
                System.out.println(inDegree.get(item._1));
                Document document = new Document("rid",Integer.parseInt(Long.toString((Long)item._1))).append("name",inDegree.get(item._1)).append("rank",item._2);
                verticesOutputList.add(document);
            }else {
                System.out.println();
            }
        });

        List<Document> edgesOutputList = new LinkedList<>();
        List<Edge<String>> edgeRDDList = new ArrayList(edges.collect());
        for (Edge<String> e:edgeRDDList){
            Document document = new Document("srcId",(int)e.srcId()).append("destId",(int)e.dstId());
            edgesOutputList.add(document);
        }

        MongoDatabase database1 = client.getDatabase("graph");
        MongoCollection<Document> outputV = database1.getCollection("vertex");
        MongoCollection<Document> outputE = database1.getCollection("edge");
        outputV.insertMany(verticesOutputList);
        outputE.insertMany(edgesOutputList);

    }
}
