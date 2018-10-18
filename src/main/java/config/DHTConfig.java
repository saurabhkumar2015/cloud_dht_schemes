package config;

import java.util.Map;

/* Doc link
 * https://docs.google.com/document/d/1zJuuDiHpp24EAniAqsfeKmsXrEXuPTuteHOf3zakkEc/edit
 */


public class DHTConfig {

    public long version;
    public int numNodeIds;
    public String proxyId;
    public boolean verbose;
    public String dhtType; // centralized or distributed
    public String scheme; // ring, elastic or ceph
    public byte replicationFactor;
    public byte cephMaxClusterSize;
    public String nodeMapLocation;
    public Map<Integer, String> nodesMap; // Node Id --> Ip:port for a datanode
}