import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.broadcast.TorrentBroadcast;
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
        JavaPairDStream<String,Tuple2<String,Integer>> cates = lines.mapToPair(rdd->{
            JSONObject object = JSON.parseObject(rdd);
            Tuple2<String,Integer> host_hot = new Tuple2<>(object.getString("name"),object.getInteger("hot"));
            return new Tuple2<>(object.getString("cate"),host_hot);
        });
        JavaPairDStream<String,List<Tuple2<String,Integer>>> cate_host_hot = cates.combineByKey(new Function<Tuple2<String, Integer>, List<Tuple2<String, Integer>>>() {
            @Override
            public List<Tuple2<String, Integer>> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                List<Tuple2<String, Integer>> list = new LinkedList<>();
                list.add(stringIntegerTuple2);
                return list;
            }
        }, new Function2<List<Tuple2<String, Integer>>, Tuple2<String, Integer>, List<Tuple2<String, Integer>>>() {
            @Override
            public List<Tuple2<String, Integer>> call(List<Tuple2<String, Integer>> tuple2s, Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                tuple2s.add(stringIntegerTuple2);
                return tuple2s;
            }
        }, new Function2<List<Tuple2<String, Integer>>, List<Tuple2<String, Integer>>, List<Tuple2<String, Integer>>>() {
            @Override
            public List<Tuple2<String, Integer>> call(List<Tuple2<String, Integer>> tuple2s, List<Tuple2<String, Integer>> tuple2s2) throws Exception {
                tuple2s.addAll(tuple2s2);
                return tuple2s;
            }
        },new HashPartitioner(2));
        JavaDStream<CateHostHot> cate_top_hot = cate_host_hot.map(new Function<Tuple2<String, List<Tuple2<String, Integer>>>, CateHostHot>() {
            @Override
            public CateHostHot call(Tuple2<String, List<Tuple2<String, Integer>>> stringListTuple2) throws Exception {
                List<Tuple2<String, Integer>> top10 = new LinkedList<>();
                List<Tuple2<String, Integer>> all = stringListTuple2._2;
                all.sort((o1, o2) -> o2._2 - o1._2);
                for (int i=0;i<10&& i<all.size();i++){
                    top10.add(all.get(i));
                }


                return new CateHostHot(new Tuple2<>(stringListTuple2._1,top10));
            }
        });

        SimpleDateFormat df = new SimpleDateFormat("HH:mm:ss");
        String[] parts = df.format(new Date()).split(":");
        Broadcast<String> time = jssc.sparkContext().broadcast(parts[0] + ":" + parts[1]);
        cate_top_hot.foreachRDD(rdd->{
            rdd.foreach(item->{
                String timeStamp = time.value();
                String cateName = item.t._1;
                MongoCollection<Document> collection = new MongoClient("172.19.241.171").getDatabase("cate_top_host").getCollection(cateName);
                Document document = new Document("cate",cateName).append("list",item.t._2);
                collection.insertOne(document);
            });
        });
        JavaPairDStream<String,Integer> curHot = hosts.reduceByKey(Integer::sum);

        JavaDStream<MyTuple2> topHot = curHot.map((Function<Tuple2<String, Integer>, MyTuple2>) MyTuple2::new);
        topHot.print(1);


        topHot.foreachRDD(rdd ->{

            String timeStamp = time.value();
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
        time.unpersist();

        jssc.start();
        jssc.awaitTermination();
    }
}
