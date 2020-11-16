import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

public class MongoTest {
    public static void main(String[] args) {
        MongoClient client = new MongoClient("172.19.241.171");
        MongoDatabase database = client.getDatabase("Hot");
        MongoCollection<Document> collection = database.getCollection("HotCount");
        System.out.println(collection.find().iterator().hasNext());
    }
}
