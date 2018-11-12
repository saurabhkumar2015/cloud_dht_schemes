package common;

import java.io.Serializable;

public class LoadBalance implements Serializable {

    public int nodeId;
    public double loadFactor;

    @Override
    public String toString() {
        return "LoadBalance{" +
                "nodeId=" + nodeId +
                ", loadFactor=" + loadFactor +
                '}';
    }
}
