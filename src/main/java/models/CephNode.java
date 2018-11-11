package models;

import java.util.List;

public class CephNode {

    public int weight;
    public boolean alive;
    public int clusterId;
    public CephNode leftNode;
    public CephNode rightNode;
    public List<CephLeafNode> nodes;


    @Override
    public String toString() {
        return "CephNode{" +
                "weight=" + weight +
                ", alive=" + alive +
                ", clusterId=" + clusterId +
                ", leftNode=" + leftNode +
                ", rightNode=" + rightNode +
                ", nodes=" + nodes +
                '}';
    }
}
