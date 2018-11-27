package common;

import config.ConfigLoader;
import org.apache.gossip.model.SharedGossipDataMessage;
import schemes.ElasticDHT.ERoutingTable;
import java.util.Date;

import static common.Commons.dateFormat;
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
                Thread.sleep(200 + ConfigLoader.config.gossipSleep);
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
                        Date date = new Date();
                        System.out.println(dateFormat.format(date) + ": GOSSIP RECEIVED NEW ROUTING TABLE FROM NODE "+ msg.getNodeId()+ " ORIGINATED FROM "+ r.originatorNodeId+ " NEW VERSION::" +r.table.getVersionNumber() + " OLD VERSION WAS:"+ oldVersion );
                        msg.setTimestamp(System.currentTimeMillis());
                        msg.setNodeId(Integer.toString(dataNode.getNodeId()));
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
                        Commons.gossip.gossipSharedData(msg);
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
