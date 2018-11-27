package config;

import java.util.List;
import java.util.Map;
import java.io.Serializable;

/* Doc link
 * https://docs.google.com/document/d/1zJuuDiHpp24EAniAqsfeKmsXrEXuPTuteHOf3zakkEc/edit
 */


public class DHTConfig implements Serializable{

    public long version;
    public int nodeIdStart= 1;
    public int nodeIdEnd= 100;
    public String proxyIp;
    public int bucketSize;
    public String verbose; // info,debug,error
    public String dhtType; // centralized or distributed
    public String scheme; // ring, elastic or ceph
    public byte replicationFactor;
    public byte cephMaxClusterSize = 5;
    //public String nodeMapLocation= "C:\\Users\\kavit\\Documents\\GitHub\\cloud_dht_schemes\\src\\main\\java\\config\\PhysicalNodeDetails.txt"; 
    public String nodeMapLocation= "C:\\Users\\kavit\\Documents\\GitHub\\cloud_dht_schemes\\nodes.csv";
    public int seed = 32;
    public int PlacementGroupMaxLimit = 10;
    public Map<Integer, String> nodesMap; // Node Id --> Ip:port for a datanode
	public int resizeFactor = 100;
    public Map<Integer, List<Integer>> gossipList;
    public int sleepTime;
    public int gossipSleep;

    @Override
    public String toString() {
        return "DHTConfig{" +
                "versionNumber=" + version +
                ", nodeIdStart=" + nodeIdStart +
                ", nodeIdEnd=" + nodeIdEnd +
                ", proxyIp='" + proxyIp + '\'' +
                ", bucketSize=" + bucketSize +
                ", verbose='" + verbose + '\'' +
                ", dhtType='" + dhtType + '\'' +
                ", scheme='" + scheme + '\'' +
                ", replicationFactor=" + replicationFactor +
                ", cephMaxClusterSize=" + cephMaxClusterSize +
                ", nodeMapLocation='" + nodeMapLocation + '\'' +
                ", seed=" + seed +
                ", nodesMap Size=" + nodesMap.size() +
                ", nodesMap=" + nodesMap + 
                ", resize Factor = " +resizeFactor +
		", sleepTime=" + sleepTime +
                "}\n\n";
    }
}
