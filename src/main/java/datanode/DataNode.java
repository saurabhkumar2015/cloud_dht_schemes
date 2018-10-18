package datanode;

import common.IStrategy;
import org.apache.log4j.Logger;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class DataNode implements IDataNode {

    private Logger log = Logger.getLogger(DataNode.class);
    private ConcurrentMap<Integer, Set<Integer>> storeMap; // HashIndex --> Set(replica ID Set)
    public int nodeId;
    public IStrategy strategy; //In case of DHT routing table update. Which is the best strategy to follow.

    public DataNode(int nodeId) {
        storeMap = new ConcurrentHashMap<Integer, Set<Integer>>();
        this.nodeId = nodeId;
        log.info("DataNode " + nodeId + " created successfully");
    }

    /**
     * Add new files i.e. hash index to DataNode
     * @param newFiles
     */
    public void addNewFiles(Map<Integer, Integer> newFiles) {
        for (Map.Entry<Integer, Integer> e : newFiles.entrySet()) {
            Integer replicaId = e.getValue();
            HashSet<Integer> replicas = new HashSet<Integer>();
            replicas.add(replicaId);
            Set<Integer> replicaSet = storeMap.putIfAbsent(e.getKey(), replicas);
            if (replicaSet != null) synchronized (replicaSet) {
                replicaSet.add(replicaId);
            }
        }
        log.debug("Add New Files Request " + newFiles + " completed successfully");
    }

    /**
     * Remove files i.e. hashindex from datanode
     * @param removeFiles
     */
    public void removeFiles(Map<Integer, Integer> removeFiles) {
        for (Map.Entry<Integer, Integer> e : removeFiles.entrySet()) {
            Set<Integer> replicaSet = storeMap.get(e.getKey());
            if (replicaSet != null) replicaSet.remove(e.getValue());
        }
        log.debug("Remove Files Request " + removeFiles + " completed successfully");
    }

    /**
     * Modify Replica Id's of a file(HashIndex)
     * @param modifyFiles
     */
    public void modifyReplicaIds(Map<Integer, Map<Integer, Integer>> modifyFiles) {
        for (Map.Entry<Integer, Map<Integer, Integer>> e : modifyFiles.entrySet()) {
            Set<Integer> replicaSet = storeMap.get(e.getKey());
            synchronized (replicaSet) {
                for (Map.Entry<Integer, Integer> ee : e.getValue().entrySet()) {
                    replicaSet.remove(ee.getKey());
                    replicaSet.add(ee.getValue());
                }
            }
        }
        log.debug("Modify Files Request " + modifyFiles + " completed successfully");
    }

    /**
     * Print the current state of datanode.
     */
    public void printStatus() {
        System.out.println("Printing Status of DataNode "+ nodeId);
        for(Map.Entry<Integer, Set<Integer>> e : storeMap.entrySet()) {
            System.out.println("Hash Index: "+ e.getKey() + " replicas: " +  e.getValue());
        }
    }
}
