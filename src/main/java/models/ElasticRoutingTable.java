package models;

import java.util.List;
import java.util.Map;

public class ElasticRoutingTable {

    public long version;
    public Map<Integer, List<Integer>> elasticRoutingMap; // HashIndex --> List<NodeIds>
    public Map<Integer, List<Integer>> invertedMap; // NodeId --> List<HashIndex>

    @Override
    public String toString() {
        return "ElasticRoutingTable{" +
                "version=" + version +
                ", elasticRoutingMap=" + elasticRoutingMap +
                ", invertedMap=" + invertedMap +
                '}';
    }
}
