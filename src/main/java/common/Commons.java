package common;

import ceph.CephDataNode;
import ceph.CephRoutingTable;
import ceph.EntryPoint;
import config.DHTConfig;
import ring.DataNode;
import ring.RingDHTScheme;
import ring.RingRoutingTable;
import schemes.ElasticDHT.DataNodeElastic;
import schemes.ElasticDHT.RoutingTable;
import socket.IMessageSend;

public class Commons {

    public static IRoutingTable loadScheme(DHTConfig config, IDataNode dataNode) throws Exception {

        String scheme = config.scheme;
        switch (scheme) {
            case "RING":
            case "ring":
                return new RingRoutingTable();
            case "ELASTIC":
            case "elastic":
                schemes.ElasticDHT.RoutingTable r = new schemes.ElasticDHT.RoutingTable();
                RoutingTable.GetInstance().getRoutingTable();
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
                return new DataNode(new RingDHTScheme());
            case "ELASTIC":
            case "elastic":
                return new DataNodeElastic();
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
                schemes.ElasticDHT.RoutingTable r = new schemes.ElasticDHT.RoutingTable();
                RoutingTable.GetInstance().getRoutingTable();
                routingTable = r;
                break;
            case "CEPH":
                EntryPoint entryPoint = new EntryPoint();
                entryPoint.BootStrapCeph();
                routingTable = CephRoutingTable.getInstance();
                break;
            default:
                throw new Exception("Incompatible DHT schema found!");
        }
        return routingTable;
    }
    
    // Message Sender
    public static IMessageSend messageSender;
    
    public static Payload GeneratePayload(String fileName, int replica)
    {
    	return new Payload(fileName, replica);
    }
}
