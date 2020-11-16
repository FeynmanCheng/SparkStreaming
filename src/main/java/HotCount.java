import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;

import org.bson.Document;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.*;

public final class HotCount {

    public static void main(String[] args) throws InterruptedException{
//        SparkConf conf = new SparkConf().setMaster("spark://master:7077").setAppName("HotCount");
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("HotCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(6));

        JavaDStream<String> files = jssc.textFileStream("hdfs://172.19.241.159:9000/user/hadoop/test");
        JavaDStream<String> lines = files.flatMap(x -> Arrays.asList(x.split(System.getProperty("line.separator"))).iterator());
        JavaPairDStream<String,Integer> hosts = lines.mapToPair(rdd->{
            JSONObject object = JSON.parseObject(rdd);
            return new Tuple2<>(object.getString("cate"),object.getInteger("hot"));
        });
        JavaPairDStream<String,Integer> curHot = hosts.reduceByKey(Integer::sum);

        JavaDStream<MyTuple2> topHot = curHot.map((Function<Tuple2<String, Integer>, MyTuple2>) MyTuple2::new);
        topHot.print(1);


        topHot.foreachRDD(rdd ->{
            SimpleDateFormat df = new SimpleDateFormat("HH:mm:ss");
            String[] parts = df.format(new Date()).split(":");
            String timeStamp = parts[0] + ":" + parts[1];
//            List<Tuple2<String,Integer>> items = rdd.items(10, (o1, o2) -> o2._2-o1._2);
//            List<Tuple2<String,Integer>> items = rdd.items(10, CompareHot.getComparator());
            List<MyTuple2> items = rdd.top(10);

            MongoClient client = new MongoClient("172.19.241.171");
            MongoDatabase database = client.getDatabase("hot");
            MongoCollection<Document> collection = database.getCollection("cate_hot");

            if (items.size()>0){
                List<Document> jsons = new LinkedList<>();
                items.forEach(t ->{
                    Document document = new Document("name",t.t._1).append("hot",t.t._2);
                    jsons.add(document);
                });

                Document document = new Document("timeStamp",timeStamp);
                document.append("list",jsons);

                collection.insertOne(document);
            }

        });


        jssc.start();
        jssc.awaitTermination();
    }
}
