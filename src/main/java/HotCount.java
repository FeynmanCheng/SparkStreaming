import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;

import org.bson.Document;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.*;

public final class HotCount {

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("HotCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(6));

        JavaDStream<String> files = jssc.textFileStream("hdfs://172.19.241.159:9000/user/hadoop/test");
        JavaDStream<String> files2 = jssc.textFileStream("hdfs://172.19.241.159:9000/user/hadoop/test");
        JavaDStream<String> lines = files.flatMap(x -> Arrays.asList(x.split(System.getProperty("line.separator"))).iterator());
        JavaDStream<String> lines2 = files2.flatMap(x -> Arrays.asList(x.split(System.getProperty("line.separator"))).iterator());
        JavaPairDStream<String, Integer> hosts = lines.mapToPair(rdd -> {
            JSONObject object = JSON.parseObject(rdd);
            return new Tuple2<>(object.getString("cate"), object.getInteger("hot"));
        });

        JavaPairDStream<String, Integer> curHot = hosts.reduceByKey(Integer::sum);

        JavaDStream<MyTuple2> topHot = curHot.map((Function<Tuple2<String, Integer>, MyTuple2>) MyTuple2::new);
        topHot.print(1);


        topHot.foreachRDD(rdd -> {
            SimpleDateFormat df = new SimpleDateFormat("HH:mm:ss");
            String[] parts = df.format(new Date()).split(":");
            String timeStamp = parts[0] + ":" + parts[1];
            List<MyTuple2> items = rdd.top(10);

            MongoClient client = new MongoClient("172.19.241.171");
            MongoDatabase database = client.getDatabase("hot");
            MongoCollection<Document> collection = database.getCollection("cate_hot");

            if (items.size() > 0) {
                List<Document> jsons = new LinkedList<>();
                items.forEach(t -> {
                    Document document = new Document("name", t.t._1).append("hot", t.t._2);
                    jsons.add(document);
                });

                Document document = new Document("timeStamp", timeStamp);
                document.append("list", jsons);

                collection.insertOne(document);
            }

        });


        JavaPairDStream<String, Tuple2<String, Integer>> cates = lines2.mapToPair(rdd -> {
            JSONObject object = JSON.parseObject(rdd);
            Tuple2<String, Integer> host_hot = new Tuple2<>(object.getString("name"), object.getInteger("hot"));
            return new Tuple2<>(object.getString("cate"), host_hot);
        });
        JavaPairDStream<String, List<Tuple2<String, Integer>>> cate_host_hot = cates.combineByKey(new Function<Tuple2<String, Integer>, List<Tuple2<String, Integer>>>() {
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
        }, new HashPartitioner(2));
        JavaDStream<CateHostHot> cate_top_hot = cate_host_hot.map(new Function<Tuple2<String, List<Tuple2<String, Integer>>>, CateHostHot>() {
            @Override
            public CateHostHot call(Tuple2<String, List<Tuple2<String, Integer>>> stringListTuple2) throws Exception {
                List<Tuple2<String, Integer>> top10 = new LinkedList<>();
                List<Tuple2<String, Integer>> all = stringListTuple2._2;
                all.sort((o1, o2) -> o2._2 - o1._2);
                int i = 0;
                while (top10.size() < 10 && i < all.size()) {
                    if (top10.size() == 0 || (!all.get(i)._1.equals(top10.get(top10.size() - 1)._1))) {
                        top10.add(all.get(i));
                    }
                    i++;
                }
                return new CateHostHot(new Tuple2<>(stringListTuple2._1, top10));
            }
        });

        cate_top_hot.foreachRDD(rdd -> {
            rdd.foreach(item -> {
                SimpleDateFormat df = new SimpleDateFormat("HH:mm:ss");
                String[] parts = df.format(new Date()).split(":");
                String timeStamp = parts[0] + ":" + parts[1];
                String cateName = item.t._1;
                MongoCollection<Document> collection = new MongoClient("172.19.241.171").getDatabase("cate_top_host").getCollection(cateName);
                List<Document> list = new LinkedList<>();
                item.t._2.forEach(i -> {
                    list.add(new Document("host", i._1).append("hot", i._2));
                });
                Document document = new Document("time", timeStamp).append("list", list);
                collection.insertOne(document);
            });
        });

        jssc.start();
        jssc.awaitTermination();
    }
}
