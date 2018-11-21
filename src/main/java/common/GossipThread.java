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
                    IRoutingTable r = null;
                    switch (ConfigLoader.config.scheme.toLowerCase()){
                        case "ceph":
                            r = (IRoutingTable) msg.getPayload();
                            break;
                        case "elastic":
                            r = (IRoutingTable) msg.getPayload();
                           break;
                        case "ring":
                            r = (IRoutingTable) msg.getPayload();
                            break;
                    }
                    if (r.getVersionNumber() > dataNode.getRoutingTable().getVersionNumber()) {
                        System.out.println("Routing Table Recieved from Gossip::" + dataNode.getRoutingTable().getVersionNumber());
                        dataNode.UpdateRoutingTable(r);
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
