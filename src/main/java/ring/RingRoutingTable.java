package ring;
import java.io.*;
import java.util.*;
import java.util.HashMap;

import common.IRoutingTable;
import config.ConfigLoader;
import config.DHTConfig;

public class RingRoutingTable implements IRoutingTable {

    public long version;
    public Map<Integer,Integer> routingMap; // HashMap for hashStartIndex to nodeId mapping
    public DHTConfig conf;
    public Map<Integer, String> physicalTable;
    private static final int MAX_HASH = 2013265907;
    public int numNodeIds;
    public byte replicationFactor;



    public RingRoutingTable(){
        this.conf = ConfigLoader.config;
        this.numNodeIds = this.conf.nodeIdEnd-this.conf.nodeIdStart+1;
        this.version = conf.version;
        this.routingMap = new TreeMap<Integer,Integer>();
        this.physicalTable = new HashMap<Integer,String>();
        this.populateTables();

        System.out.print("This is the initial routing table which will be available at every data node\n");
        this.printRoutingTable();
        System.out.print("\n");
        System.out.print("This is the initial NodeId-Physical Machine mapping table\n");
        this.printPhysicalTable();

    }

    @Override
    public String toString() {
        return "RingRoutingTable{" +
                "version=" + version +
                ", routingMap=" + routingMap +
                '}';
    }

    //Hash generator for given string
    public int getHasValueFromIpPort(String ipPort) {
        return Math.abs((ipPort.hashCode())%MAX_HASH);
    }

    //initiating physical table and routing map
    public void populateTables() {
        int nodeId =0;
        try {
            for ( Map.Entry<Integer, String> e : this.conf.nodesMap.entrySet())
            {
                this.physicalTable.put(e.getKey(), e.getValue());
                //Generate random hash for every IP:Port
                int hashVal = this.getHasValueFromIpPort(e.getValue());
                this.routingMap.put(hashVal, nodeId);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        //this.numNodeIds = nodeId;
        //System.out.println(this.getNodeId("asddfr3rgerg",3));
    }

    /*Find nodeId corresponding to given hashval
    Binary search done on routing table (Tree map)
    */
    public void printRoutingTable() {
        System.out.println("HashVal\tNodeId");
        for (Map.Entry<Integer, Integer> e : this.routingMap.entrySet()) {
            System.out.print(e.getKey());
            System.out.print("\t");
            System.out.println(e.getValue());
        }
    }

    public void printPhysicalTable() {
        System.out.println("NodeId\tNodeIp_Port");
        for (Map.Entry<Integer, String> e : this.physicalTable.entrySet()) {
            System.out.print(e.getKey());
            System.out.print("\t");
            System.out.println(e.getValue());
        }
    }


    /*Find nodeId corresponding to given hashval
    Binary search done on routing table (Tree map)
    */
    public LinkedList<Integer> modifiedBinarySearch(int findHashVal){
        System.out.println("Searching Hash Val: "+findHashVal);

        LinkedList<Integer> listOfHash =  new LinkedList<Integer>();
        listOfHash.addAll(this.routingMap.keySet());
        LinkedList<Integer> listOfNodesForGivenHash = new LinkedList<Integer>();
        LinkedList<Integer> listOfHashesForGivenHash = new LinkedList<Integer>();
        int start = 0;
        int end = this.routingMap.size()-1;
        int returnHashValIndex = 0;
        while(start<=end) {
            int mid = (start+end)/2;
            int midVal = listOfHash.get(mid);
            if(midVal==findHashVal) {
                System.out.println("found hash: "+midVal);
                listOfNodesForGivenHash.add(this.routingMap.get(midVal));
                listOfHashesForGivenHash.add(midVal);
                returnHashValIndex = mid;
                break;
            }
            else if(midVal>findHashVal) {
                //System.out.println("first half");
                end = mid-1;
                int nextVal = listOfHash.get(end);
                if(nextVal<findHashVal) {
                    System.out.println("found hash: "+nextVal);
                    listOfNodesForGivenHash.add(this.routingMap.get(nextVal));
                    listOfHashesForGivenHash.add(nextVal);
                    returnHashValIndex = end;
                    break;
                }
            }
            else {
                //System.out.println("second half");
                start = mid+1;
                int nextVal = listOfHash.get(start);
                if(nextVal>findHashVal) {
                    System.out.println("found hash: "+midVal);
                    listOfNodesForGivenHash.add(this.routingMap.get(midVal));
                    listOfHashesForGivenHash.add(midVal);
                    returnHashValIndex = start;
                    break;
                }
            }
        }

        //add successors
        //System.out.println("returnHashValIndex: "+returnHashValIndex);
        for (byte j = 0; j< this.replicationFactor-1; j++) {
            listOfNodesForGivenHash.add(this.routingMap.get(listOfHash.get((returnHashValIndex+j)%this.numNodeIds)));
            listOfHashesForGivenHash.add(listOfHash.get((returnHashValIndex+j)%this.numNodeIds));
        }
        System.out.println("\n");
        //Print List of nodes associated with given hash value
        System.out.println("List of nodes under consideration");
        for (int i=0; i<listOfNodesForGivenHash.size();i++) {
            System.out.println("NodeId: "+listOfNodesForGivenHash.get(i)+" hashStartValue: "+listOfHashesForGivenHash.get(i));
        }

        //return listOfNodesForGivenHash;
        return listOfHashesForGivenHash;
    }

    //Find Node corresponding to given filename
    public int getNodeId(String fileName, int replicationId) {
        int hashVal = this.getHasValueFromIpPort(fileName);
        LinkedList<Integer> listOfNodesForGivenHash = modifiedBinarySearch(hashVal);
        return listOfNodesForGivenHash.get(replicationId-1);

    }

    @Override
    public IRoutingTable addNode(int nodeId) {
        return null;
    }

    @Override
    public IRoutingTable deleteNode(int nodeId) {
        return null;
    }

    @Override
    public IRoutingTable loadBalance(int nodeId, double loadFactor) {
        return null;
    }
}