package datanode;

import java.util.List;

public class FileRequest {
    public List<Integer> hashIndexes;
    public Integer startRange;
    public Integer endRange;
    public Integer replicaId;
    public Integer newReplicaId;

    @Override
    public String toString() {
        return "FileRequest{" +
                "hashIndexes=" + hashIndexes +
                ", startRange=" + startRange +
                ", endRange=" + endRange +
                ", replicaId=" + replicaId +
                ", newReplicaId=" + newReplicaId +
                '}';
    }
}
