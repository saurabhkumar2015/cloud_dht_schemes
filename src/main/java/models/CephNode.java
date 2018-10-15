package models;

public class CephNode {

    public int weight;
    public boolean alive;
    public int clusterId;
    public CephNode leftNode;
    public CephNode rightNode;

    @Override
    public String toString() {
        return "CephNode{" +
                "weight=" + weight +
                ", alive=" + alive +
                ", clusterId=" + clusterId +
                ", leftNode=" + leftNode +
                ", rightNode=" + rightNode +
                '}';
    }
}
