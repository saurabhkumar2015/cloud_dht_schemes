package common;

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
                System.out.println("Lives:"+Commons.gossip.getGossipManager().getLiveMembers().size());
                Thread.sleep(2000L);
                SharedGossipDataMessage msg  = Commons.gossip.findSharedData(ROUTING_TABLE);
                System.out.println("Gossip Msg::"+ msg);
                if(msg != null) {
                    IRoutingTable r = (IRoutingTable) msg.getPayload();
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
