package testgossip;

import com.codahale.metrics.MetricRegistry;
import org.apache.gossip.*;
import org.apache.gossip.model.SharedGossipDataMessage;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.*;

public class TestGossip {

    public static void main(String[] args) throws URISyntaxException, UnknownHostException, InterruptedException {

        String host = args[0];
        Integer port = Integer.parseInt(args[1]);
        String cluster = args[2];
        String mode = args[4];

        GossipSettings settings = new GossipSettings();
        settings.setGossipInterval(100);
        settings.setCleanupInterval(1000);
        settings.setWindowSize(100);
        settings.setPersistDataState(false);
        settings.setPersistRingState(false);
        settings.setConvictThreshold(2.00);


        List<GossipMember> startupMembers = new ArrayList<>();
        for (int i = 0; i < 5; ++i) {
            if(port.equals(50000+i)) continue;
            URI uri = new URI("udp://" + host + ":" + (50000 + i));
            startupMembers.add(new RemoteGossipMember(cluster, uri, i + ""));
        }

        URI uri = new URI("udp://" + "127.0.0.1" + ":" + (port ));
        GossipService gossipService = new GossipService(cluster, uri, port-50000 + "",
                new HashMap<String, String>(), startupMembers, settings, null, new MetricRegistry());
        gossipService.start();


        SharedGossipDataMessage message = new SharedGossipDataMessage();
        message.setExpireAt(System.currentTimeMillis()+60000);
        message.setKey("OSD_MAP");
        String nodeId = Integer.toString(port-50000);
        message.setNodeId(nodeId);
        Map<String,String> map = new HashMap<>();

        int i = Integer.parseInt(args[3]);
        while(true) {
            System.out.println("\n");
            if(mode.equals("w")) {
                message.setTimestamp(System.currentTimeMillis());
                map = new HashMap<>();
                map.put(nodeId, Integer.toString(i++));
                message.setPayload(map);
                gossipService.gossipSharedData(message);
                System.out.println("Sent Gossip Message::" + message.getPayload());
                message.setExpireAt(System.currentTimeMillis()+120000);
            }
            System.out.print("Live Nodes: ");
            for (LocalGossipMember each: gossipService.getGossipManager().getLiveMembers()) {
                System.out.print(each.toString().substring(0,45) + " ");
            }
            System.out.println("\n");

            Thread.sleep(1000L);
            SharedGossipDataMessage msg = gossipService.findSharedData("OSD_MAP");
            if (msg != null)System.out.println("Current state of OSD_MAP is:" + msg.getPayload());
            Thread.sleep(3000L);
        }
    }
}
