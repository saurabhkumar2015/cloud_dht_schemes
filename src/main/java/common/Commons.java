package common;

import ceph.CephDataNode;
import ceph.CephRoutingTable;
import config.DHTConfig;
import ring.RingRoutingTable;
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
                return null;
            case "ELASTIC":
            case "elastic":
                return null;
            case "CEPH":
            case "ceph":
                return new CephDataNode(nodeId);
            default:
                throw new Exception("Unsupported DHT scheme found " + config.scheme);

        }
    }
    
        // Message Sender
    public static IMessageSend messageSender;
    
    public static Payload GeneratePayload(String fileName, int pgGroup, int replica)
    {
    	return new Payload(fileName, pgGroup, replica);
    }
}
