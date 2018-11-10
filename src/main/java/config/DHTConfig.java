package config;

import java.util.Map;

/* Doc link
 * https://docs.google.com/document/d/1zJuuDiHpp24EAniAqsfeKmsXrEXuPTuteHOf3zakkEc/edit
 */


public class DHTConfig {

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
    public String nodeMapLocation;
    public int seed = 32;
    public int PlacementGroupMaxLimit = 10;
    public Map<Integer, String> nodesMap; // Node Id --> Ip:port for a datanode

    @Override
    public String toString() {
        return "DHTConfig{" +
                "version=" + version +
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
                "}\n\n";
    }
}
