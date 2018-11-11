package models;

public class CephRoutingTable {

    public long version;
    public CephNode tree; //root of the tree

    @Override
    public String toString() {
        return "CephRoutingTable{" +
                "version=" + version +
                ", tree=" + tree +
                '}';
    }
}
