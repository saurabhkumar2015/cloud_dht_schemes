import datanode.DataNode;

import java.util.ArrayList;
import java.util.List;

public class ReplicationSimulator {

    public static void printTheCurrentState(List<DataNode> datanodes) {
        for (DataNode dn : datanodes){
            dn.printStatus();
        }
    }

    /**
     * Number of hashindexes in a datanode
     *
     *
     * @param n
     * @return
     */
    public static List<DataNode> getDataNodeList(int n) {

        List<DataNode> ls = new ArrayList<DataNode>();
        for (int i =1 ; i <=n; i++) {
            ls.add(new DataNode(i));
        }
        return ls;
    }

}
