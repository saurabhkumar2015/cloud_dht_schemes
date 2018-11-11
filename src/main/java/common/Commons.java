package common;

import ceph.CephDataNode;
import ceph.CephRoutingTable;
import config.DHTConfig;
import common.IDataNode;
import ring.RingRoutingTable;
import schemes.ElasticDHT.RoutingTable;

public class Commons {

    public static IRoutingTable loadScheme(DHTConfig config, IDataNode dataNode) throws Exception {

        String scheme = config.scheme;
        switch (scheme) {
            case "RING":
            case "ring":
                return new RingRoutingTable();
            case "ELASTIC":
            case "elastic":
                return new RoutingTable();
            case "CEPH":
            case "ceph":
                CephRoutingTable cephRoutingTable = new CephRoutingTable();
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
}
