package models;

import java.util.Map;

public class RingRoutingTable {

    public long version;
    public Map<Integer,Integer> routingMap; // HashMap for hashIndex to node id mapping

    @Override
    public String toString() {
        return "RingRoutingTable{" +
                "version=" + version +
                ", routingMap=" + routingMap +
                '}';
    }
}
