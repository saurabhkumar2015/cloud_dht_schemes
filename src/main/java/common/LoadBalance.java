package common;

import java.io.Serializable;

public class LoadBalance implements Serializable {

    public int nodeId;
    public double loadFactor;

    public LoadBalance(int nodeId, double loadFactor) {
        this.nodeId = nodeId;
        this.loadFactor = loadFactor;
    }

    @Override
    public String toString() {
        return "LoadBalance{" +
                "nodeId=" + nodeId +
                ", loadFactor=" + loadFactor +
                '}';
    }
}
