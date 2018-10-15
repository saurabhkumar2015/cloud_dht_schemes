package datanode;

import common.IStrategy;

import java.util.Map;
import java.util.Set;

public class DataNode implements IDataNode {

    public Map<Integer, Set<Integer>> storeMap; // HashIndex --> Set(replica ID Set)
    public IStrategy strategy;

}
