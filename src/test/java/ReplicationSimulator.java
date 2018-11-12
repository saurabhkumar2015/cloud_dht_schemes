import datanode.DataNodeDeleted;

import java.util.ArrayList;
import java.util.List;

public class ReplicationSimulator {

    public static void printTheCurrentState(List<DataNodeDeleted> datanodes) {
        for (DataNodeDeleted dn : datanodes){
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
    public static List<DataNodeDeleted> getDataNodeList(int n) {

        List<DataNodeDeleted> ls = new ArrayList<DataNodeDeleted>();
        for (int i =1 ; i <=n; i++) {
            ls.add(new DataNodeDeleted(i));
        }
        return ls;
    }

}
