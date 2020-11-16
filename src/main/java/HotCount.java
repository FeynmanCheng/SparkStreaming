import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;

import org.bson.Document;
import scala.Tuple2;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
public final class HotCount {

    public static void main(String[] args) throws InterruptedException{
//        SparkConf conf = new SparkConf().setMaster("spark://master:7077").setAppName("HotCount");
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("HotCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(50));

        JavaDStream<String> files = jssc.textFileStream("hdfs://172.19.241.159:9000/user/hadoop/test");
        JavaDStream<String> lines = files.flatMap(x -> Arrays.asList(x.split(System.getProperty("line.separator"))).iterator());
        JavaPairDStream<String,Integer> hosts = lines.mapToPair(rdd->{
            JSONObject object = JSON.parseObject(rdd);
            return new Tuple2<>(object.getString("cate"),object.getInteger("hot"));
        });
        JavaPairDStream<String,Integer> curHot = hosts.reduceByKey(Integer::sum);

        curHot.print(10);



        curHot.foreachRDD(rdd ->{
            MongoClient client = new MongoClient("172.19.241.171");
            MongoDatabase database = client.getDatabase("hot");
            MongoCollection<Document> collection = database.getCollection("hot_count");
            LocalDateTime time = LocalDateTime.now();
//            List<Tuple2<String,Integer>> top = rdd.top(10, (o1, o2) -> o2._2-o1._2);
//            List<Tuple2<String,Integer>> top = rdd.top(10, CompareHot.getComparator());
            List<Tuple2<String,Integer>> top = rdd.collect();


            if (top.size()>0){
                List<Document> jsons = new LinkedList<>();
                top.forEach(t ->{
                    Document document = new Document("name",t._1).append("hot",t._2);
                    jsons.add(document);
                });
                collection.insertMany(jsons);


                MongoCursor<Document> cursor = collection.find().sort(new BasicDBObject("hot",-1)).limit(10).iterator();
                List<Document> topList = new LinkedList<>();
                while (cursor.hasNext()){
                    topList.add(cursor.next());
                }
                collection.drop();
                System.out.println(topList);
                MongoCollection<Document> newCollection = database.getCollection("HotCount");
                newCollection.insertMany(topList);
            }

        });


        jssc.start();
        jssc.awaitTermination();
    }
}
