import com.alibaba.fastjson.JSON;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;

import org.bson.Document;
import scala.Tuple2;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
public final class HotCount {
    public static void main(String[] args) throws InterruptedException{
//        SparkSession spark = SparkSession.builder()
//                .master("local")
//                .appName("MongoSparkConnectorIntro")
//                .config("spark.mongodb.input.uri", "mongodb://172.19.241.171/Hot.HotCount")
//                .config("spark.mongodb.output.uri", "mongodb://172.19.241.171/Hot.HotCount")
//                .getOrCreate();

//        SparkConf conf = new SparkConf().setMaster("spark://master:7077").setAppName("HotCount");
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("HotCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));

        JavaDStream<String> files = jssc.textFileStream("hdfs://172.19.241.159:9000/user/hadoop/test");
        JavaDStream<String> lines = files.flatMap(x -> Arrays.asList(x.split(System.getProperty("line.separator"))).iterator());
        JavaPairDStream<String,Integer> hosts = lines.mapToPair(rdd->{
            HostHot hostHot = JSON.parseObject(rdd,HostHot.class);
            return new Tuple2<>(hostHot.name,hostHot.hot);
        });
        JavaPairDStream<String,Integer> curHot = hosts.reduceByKey(Integer::sum);

        curHot.foreachRDD(rdd ->{
            MongoClient client = new MongoClient("172.19.241.171");
            MongoDatabase database = client.getDatabase("Hot");
            MongoCollection<Document> collection = database.getCollection("HotCount");

            List<Tuple2<String,Integer>> top = rdd.top(10, (r1,r2)-> r2._2 - r1._2);
            if (top.size()>0){
                List<Document> jsons = new LinkedList<>();
                top.forEach(t ->{
                    Document document = new Document("name",t._1).append("hot",t._2);
                    jsons.add(document);
                });
                collection.insertMany(jsons);
            }
            MongoCursor<Document> cursor = collection.find().sort(new BasicDBObject("hot",-1)).limit(10).iterator();
            List<Document> topList = new LinkedList<>();
            while (cursor.hasNext()){
                topList.add(cursor.next());
            }
            collection.drop();
            System.out.println(topList);
            MongoCollection<Document> newCollection = database.getCollection("HotCount");
            newCollection.insertMany(topList);
        });

        curHot.print();

        jssc.start();
        jssc.awaitTermination();
    }
}
