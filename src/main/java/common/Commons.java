package common;

import ceph.CephRoutingTable;
import config.DHTConfig;
import ring.RingRoutingTable;
import schemes.ElasticDHT.RoutingTable;

public class Commons {

    public static IRoutingTable loadScheme(DHTConfig config) throws Exception {

        String scheme = config.scheme;
        switch(scheme) {
            case "RING" :
                return new RingRoutingTable();
            case "ELASTIC" :
                return new RoutingTable();
            case "CEPH" :
                return new CephRoutingTable();
            default:
                throw new Exception("Unsupported DHT scheme found " + config.scheme);
        }
    }
}
