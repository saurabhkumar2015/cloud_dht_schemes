package models;

public class CephLeafNode {

    public int weight;
    public int clusterId;
    public int alive;

    @Override
    public String toString() {
        return "CephLeafNode{" +
                "weight=" + weight +
                ", clusterId=" + clusterId +
                ", alive=" + alive +
                '}';
    }
}
