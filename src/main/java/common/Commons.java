package common;

import ceph.*;
import config.DHTConfig;
import org.apache.gossip.GossipService;
import ring.DataNode;
import ring.RingDHTScheme;
import ring.RingRoutingTable;
import schemes.ElasticDHT.DataNodeElastic;
import schemes.ElasticDHT.ERoutingTable;
import schemes.ElasticDHT.ElasticRoutingTable;
import schemes.ElasticDHT.ElasticRoutingTableInstance;
import socket.IMessageSend;
import socket.MessageSendImpl;

import java.util.Random;

public class Commons {

    public static IRoutingTable loadScheme(DHTConfig config, IDataNode dataNode) throws Exception {

        String scheme = config.scheme;
        switch (scheme) {
            case "RING":
            case "ring":
                return new RingRoutingTable();
            case "ELASTIC":
            case "elastic":
                ERoutingTable r = new ERoutingTable();
                ERoutingTable.giveInstance().giveRoutingTable();
                return r;
            case "CEPH":
            case "ceph":
                return new CephRoutingTable();
            default:
                throw new Exception("Unsupported DHT scheme found " + config.scheme);
        }
    }

    public static IDataNode loadDataNode(DHTConfig config, int nodeId) throws Exception {

        String scheme = config.scheme;
        switch (scheme) {
            case "RING":
            case "ring":
                return new DataNode(nodeId);
            case "ELASTIC":
            case "elastic":
                IDataNode d = new DataNodeElastic(nodeId);
                Commons.elasticERoutingTable = ERoutingTable.giveInstance();
                Commons.elasticOldERoutingTable = ERoutingTable.giveInstance();
                return d;
            case "CEPH":
            case "ceph":
                return new CephDataNode(nodeId);
            default:
                throw new Exception("Unsupported DHT scheme found " + config.scheme);

        }
    }

    public static IRoutingTable initRoutingTable(DHTConfig config) throws Exception {
        String scheme = config.scheme;
        IRoutingTable routingTable;

        switch (scheme.toUpperCase().trim()) {
            case "RING":
                RingDHTScheme ring = new RingDHTScheme();
                DataNode dNode = new DataNode(ring);
                routingTable = dNode.routingTableObj;
                break;
            case "ELASTIC":
                ERoutingTable.giveInstance().giveRoutingTable();
                routingTable = ERoutingTable.giveInstance();
                break;
            case "CEPH":
                EntryPoint entryPoint = new EntryPoint();
                entryPoint.BootStrapCeph();
                routingTable = CephRoutingTable.giveInstance();
                break;
            default:
                throw new Exception("Incompatible DHT schema found!");
        }
        return routingTable;
    }
    
    // Message Sender
    public static IMessageSend messageSender = new MessageSendImpl();
    public static GossipService gossip;
    public static int nodeId;
    public static CephRoutingTable cephRoutingTable = null;
    public static OsdMap osdMap = null;
    public static HashGenerator hashGenerator = null;
    public static Random randomGen;
    public static ElasticRoutingTable elasticTable1 = new ElasticRoutingTable();

    public static ERoutingTable elasticERoutingTable = null;
    public static ERoutingTable elasticOldERoutingTable = null;

    public static Payload GeneratePayload(String fileName, int replica, long versionNo)
    {
    	return new Payload(fileName, replica, versionNo);
    }
}
