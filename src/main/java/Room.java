import java.io.Serializable;
import java.util.List;

public class Room implements Serializable {
    String hostName;
    Long rid;

    List<AnchorFriend> neighbours;
}
