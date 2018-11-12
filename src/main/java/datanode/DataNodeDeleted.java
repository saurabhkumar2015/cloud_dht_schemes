package datanode;

import common.IStrategy;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class DataNodeDeleted {

    private Logger log = Logger.getLogger(DataNodeDeleted.class);
    public ConcurrentMap<Integer, Set<Integer>> storeMap; // HashIndex --> Set(replica ID Set)
    public int nodeId;
    public IStrategy strategy; //In case of DHT routing table update. Which is the best strategy to follow.

    public DataNodeDeleted(int nodeId) {
        storeMap = new ConcurrentHashMap<Integer, Set<Integer>>();
        this.nodeId = nodeId;
        log.info("DataNodeDeleted " + nodeId + " created successfully");
    }

    /**
     * @param newFiles  Add new files i.e. hash index to DataNodeDeleted
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
     * @param removeFiles Remove files i.e. hashindex from datanode Map <HashIndex, ReplicaId>
     */
    public void removeFiles(Map<Integer, Integer> removeFiles) {
        for (Map.Entry<Integer, Integer> e : removeFiles.entrySet()) {
            Set<Integer> replicaSet = storeMap.get(e.getKey());
            if (replicaSet != null){
                replicaSet.remove(e.getValue());
                if (replicaSet.size() ==0) storeMap.remove(e.getKey());
            }
        }
        log.debug("Remove Files Request " + removeFiles + " completed successfully");
    }

    /**
     * @param modifyFiles Modify Replica Id's of a file(HashIndex)
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
     * @param requests Remove Hash Index of the files from datanode
     */
    public void editFiles(List<FileRequest> requests) {

        for(FileRequest f : requests) {
            if (f.hashIndexes == null || f.hashIndexes.size() == 0) {
                if(f.startRange == null ) continue;
                f.hashIndexes = new ArrayList<Integer>(f.endRange - f.startRange + 1);
                for (int i = f.startRange; i <= f.endRange; i++) {
                    f.hashIndexes.add(i);
                }
            }
            for (Integer index : f.hashIndexes) {
                Set<Integer> replicas = storeMap.get(index);
                if (replicas != null && replicas.contains(f.replicaId)) {
                    synchronized (replicas) {
                        replicas.remove(f.replicaId);
                        if(f.newReplicaId != -1) replicas.add(f.newReplicaId);
                        if (replicas.size() == 0) storeMap.remove(index);
                    }
                }
            }
        }
    }


    /**
     * @param requests Remove Hash Index of the files from datanode
     */
    public Map<Integer, List<Integer>> getAndEditFiles(List<FileRequest> requests) {
        Map<Integer, List<Integer>> indexesStored = new HashMap<Integer, List<Integer>>();
        for(FileRequest f : requests) {
            if (f.hashIndexes == null || f.hashIndexes.size() == 0) {
                if(f.startRange == null ) continue;
                f.hashIndexes = new ArrayList<Integer>(f.endRange - f.startRange + 1);
                for (int i = f.startRange; i <= f.endRange; i++) {
                    f.hashIndexes.add(i);
                }
            }
            List<Integer> indexes = new ArrayList<Integer>();
            indexesStored.put(f.replicaId, indexes);
            for (Integer index : f.hashIndexes) {
                Set<Integer> replicas = storeMap.get(index);
                if (replicas != null && replicas.contains(f.replicaId)) {
                    synchronized (replicas) {
                        indexes.add(index);
                        replicas.remove(f.replicaId);
                        if(f.newReplicaId != -1) replicas.add(f.newReplicaId);
                        if (replicas.size() == 0) storeMap.remove(index);
                    }
                }
            }
        }
        return indexesStored;
    }

    /**
     * Print the current state of datanode.
     */
    public void printStatus() {
        System.out.println("\nPrinting Status of DataNodeDeleted "+ nodeId);
        for(Map.Entry<Integer, Set<Integer>> e : storeMap.entrySet()) {
            System.out.println("Hash Index: "+ e.getKey() + " replicas: " +  e.getValue());
        }
    }
}
