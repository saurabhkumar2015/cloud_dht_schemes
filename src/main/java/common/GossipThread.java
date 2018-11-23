package common;

import config.ConfigLoader;
import org.apache.gossip.model.SharedGossipDataMessage;
import static common.Constants.ROUTING_TABLE;

public class GossipThread extends Thread{

    private IDataNode dataNode;
    public GossipThread (IDataNode dataNode) {
        this.dataNode = dataNode;
    }

    @Override
    public void run() {
        while(true) {
            try {
                Thread.sleep(250L);
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
                                dataNode.newUpdatedRoutingTable(r.nodeId, r.type, r.table);
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
