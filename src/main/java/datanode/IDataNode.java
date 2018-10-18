package datanode;

import java.util.Map;

public interface IDataNode {


    // newFiles File-Hash-Index --> ReplicaId
    public void addNewFiles(Map<Integer, Integer> newFiles);

    // removefiles -->File-Hash-Index --> ReplicaId
    public void removeFiles(Map<Integer, Integer> removeFiles);

    // removefiles -->File-Hash-Index --> Map < oldRelicaId -->NewReplicaId >
    public void modifyReplicaIds(Map<Integer, Map<Integer,Integer>> modifyFiles);

    public final static int PRIMARY_REPLICA = 1;
}
