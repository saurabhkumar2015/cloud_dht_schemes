package models;

public class CephRoutingTable {

    public long version;
    public CephNode tree; //root of the tree

    @Override
    public String toString() {
        return "CephRoutingTable{" +
                "versionNumber=" + version +
                ", tree=" + tree +
                '}';
    }
}
