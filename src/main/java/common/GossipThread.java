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
                Thread.sleep(2000L);
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
                    if (r.table.getVersionNumber() > dataNode.getRoutingTable().getVersionNumber()) {
                        System.out.println("Routing Table Recieved from Gossip::" + dataNode.getRoutingTable().getVersionNumber());
                        msg.setTimestamp(System.currentTimeMillis());
                        Commons.gossip.gossipSharedData(msg);
                        switch (ConfigLoader.config.scheme.toLowerCase()) {
                            case "ceph":
                                dataNode.UpdateRoutingTable(r.table, r.type );
                                break;
                            case "elastic":
                                dataNode.UpdateRoutingTable(r.table, r.type );
                                break;
                            case "ring":
                                dataNode.UpdateRoutingTable(r.table, r.type );
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
