package common;

import config.ConfigLoader;
import org.apache.gossip.model.SharedGossipDataMessage;
import schemes.ElasticDHT.DataNodeElastic;
import schemes.ElasticDHT.ERoutingTable;

import static common.Constants.*;

public class GossipThread extends Thread{

    private IDataNode dataNode;
    public GossipThread (IDataNode dataNode) {
        this.dataNode = dataNode;
    }

    @Override
    public void run() {
        while(true) {
            try {
                Thread.sleep(200L);
                SharedGossipDataMessage msg  = Commons.gossip.findSharedData(ROUTING_TABLE);
                if(msg != null) {
                    RoutingTableWrapper r = null;
                    switch (ConfigLoader.config.scheme.toLowerCase()){
                        case "ceph":
                            r = (RoutingTableWrapper) msg.getPayload();
                            break;
                        case "elastic":
                            r = (RoutingTableWrapper) msg.getPayload();
                           break;
                        case "ring":
                            r = (RoutingTableWrapper) msg.getPayload();
                            break;
                    }
                    long oldVersion = dataNode.getRoutingTable().getVersionNumber();
                    if (r.table.getVersionNumber() > oldVersion) {
                        System.out.println("GOSSIP RECEIVED NEW ROUTING TABLE VERSION::" +r.table.getVersionNumber() + " OLD VERSION WAS:"+ oldVersion );
                        msg.setTimestamp(System.currentTimeMillis());
                        Commons.gossip.gossipSharedData(msg);
                        switch((ConfigLoader.config.scheme).toUpperCase()) {
                            case "ELASTIC":
                                switch (r.type.toUpperCase()) {
                                    case ADD_NODE:
                                        dataNode.addNode(r.nodeId);
                                        break;
                                    case DELETE_NODE:
                                        dataNode.deleteNode(r.nodeId);
                                        break;
                                    case LOAD_BALANCE:
                                        dataNode.loadBalance(r.nodeId, r.factor);
                                        break;
                                }
                                Commons.elasticOldERoutingTable = Commons.elasticERoutingTable;
                                Commons.elasticERoutingTable = (ERoutingTable) r.table;
                                break;
                            case "CEPH":
                                dataNode.UpdateRoutingTable(r.table,r.type);
                                break;
                            case "RING":
                                dataNode.UpdateRoutingTable(r.table,r.type);
                                break;
                        }
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
