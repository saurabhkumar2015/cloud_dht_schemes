package common;

import java.io.Serializable;

public class RoutingTableWrapper implements Serializable {

    public IRoutingTable table;
    public String type;
    public Integer nodeId;
    public double factor;
    public Integer originatorNodeId;
}
