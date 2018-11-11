package ring;

import java.util.*;
import java.util.HashMap;

import common.IRoutingTable;
import config.ConfigLoader;
import config.DHTConfig;

public class RingRoutingTable implements IRoutingTable {

    public long version;
    public Map<Integer, Integer> routingMap; // HashMap for hashStartIndex to nodeId mapping
    public DHTConfig conf;
    public Map<Integer, String> physicalTable;
    private static final int MAX_HASH = 2013265907;
    private int numNodeIds;


    public RingRoutingTable() {
        this.conf = ConfigLoader.config;
        this.numNodeIds = this.conf.nodeIdEnd - this.conf.nodeIdStart + 1;
        this.version = conf.version;
        this.routingMap = new TreeMap<Integer, Integer>();
        this.physicalTable = new HashMap<Integer, String>();
        this.populateTables();

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
        return Math.abs((ipPort.hashCode()) % MAX_HASH);
    }

    //initiating physical table and routing map
    public void populateTables() {
        int nodeId = 0;
        try {
            for (Map.Entry<Integer, String> e : this.conf.nodesMap.entrySet()) {
                this.physicalTable.put(e.getKey(), e.getValue());
                //Generate random hash for every IP:Port
                int hashVal = this.getHasValueFromIpPort(e.getValue());
                this.routingMap.put(hashVal, nodeId);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        //this.numNodeIds = nodeId;
        //System.out.println(this.getNodeId("asddfr3rgerg",3));
    }

    /*Find nodeId corresponding to given hashval
    Binary search done on routing table (Tree map)
    */
    public LinkedList<Integer> modifiedBinarySearch(int findHashVal) {
        System.out.println(findHashVal);

        LinkedList<Integer> listOfHash = new LinkedList<Integer>();
        listOfHash.addAll(this.routingMap.keySet());
        LinkedList<Integer> listOfNodesForGivenHash = new LinkedList<Integer>();
        LinkedList<Integer> listOfHashesForGivenHash = new LinkedList<Integer>();
        int start = 0;
        int end = this.routingMap.size() - 1;

        while (start <= end) {

            int mid = (start + end) / 2;
            //System.out.println("start"+start);
            //System.out.println("end"+end);
            //System.out.println("mid"+mid);
            int midVal = listOfHash.get(mid);
            if (midVal == findHashVal) {
                System.out.println("found hash" + midVal);
                listOfNodesForGivenHash.add(this.routingMap.get(midVal));
                listOfHashesForGivenHash.add(midVal);
                //add successors
                listOfNodesForGivenHash.add(this.routingMap.get(listOfHash.get((mid + 1) % this.numNodeIds)));
                listOfHashesForGivenHash.add(listOfHash.get((mid + 1) % this.numNodeIds));
                listOfNodesForGivenHash.add(this.routingMap.get(listOfHash.get((mid + 2) % this.numNodeIds)));
                listOfHashesForGivenHash.add(listOfHash.get((mid + 2) % this.numNodeIds));
                break;
            } else if (midVal > findHashVal) {
                //System.out.println("first half");
                end = mid - 1;
                int nextVal = listOfHash.get(end);
                if (nextVal <= findHashVal) {
                    System.out.println("found hash" + nextVal);
                    listOfNodesForGivenHash.add(this.routingMap.get(nextVal));
                    listOfHashesForGivenHash.add(nextVal);
                    //add successors
                    listOfNodesForGivenHash.add(this.routingMap.get(listOfHash.get(mid)));
                    listOfHashesForGivenHash.add(listOfHash.get(mid));
                    listOfNodesForGivenHash.add(this.routingMap.get(listOfHash.get((mid + 1) % this.numNodeIds)));
                    listOfHashesForGivenHash.add(listOfHash.get((mid + 1) % this.numNodeIds));
                    break;
                }
            } else {
                //System.out.println("second half");
                start = mid;
                int nextVal = listOfHash.get(start);
                if (nextVal >= findHashVal) {
                    System.out.println("found hash" + nextVal);
                    listOfNodesForGivenHash.add(this.routingMap.get(nextVal));
                    listOfHashesForGivenHash.add(listOfHash.get(nextVal));
                    //add successors
                    listOfNodesForGivenHash.add(this.routingMap.get(listOfHash.get(mid)));
                    listOfHashesForGivenHash.add(listOfHash.get(mid));
                    listOfNodesForGivenHash.add(this.routingMap.get(listOfHash.get((mid + 1) % this.numNodeIds)));
                    listOfHashesForGivenHash.add(listOfHash.get((mid + 1) % this.numNodeIds));
                    break;
                }
            }
        }
        //List of nodes associated with given hash value

        for (int i : listOfNodesForGivenHash) {
            System.out.println("node: " + i);
        }

        for (int hash : listOfHashesForGivenHash) {
            System.out.println("hash: " + hash);
        }

        //return listOfNodesForGivenHash;
        return listOfHashesForGivenHash;
    }

    //Find Node corresponding to given filename
    public int getNodeId(String fileName, int replicationId) {
        int hashVal = this.getHasValueFromIpPort(fileName);
        LinkedList<Integer> listOfNodesForGivenHash = modifiedBinarySearch(hashVal);
        return listOfNodesForGivenHash.get(replicationId - 1);

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