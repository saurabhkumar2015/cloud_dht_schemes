package common;

import java.io.Serializable;

public class DistributedPayload implements Serializable {
    public Integer nodeId;
    public long version;
    public double loadFactor;

    @Override
    public String toString() {
        return "DistributedPayload{" +
                "nodeId=" + nodeId +
                ", version=" + version +
                ", loadFactor=" + loadFactor +
                '}';
    }
}
