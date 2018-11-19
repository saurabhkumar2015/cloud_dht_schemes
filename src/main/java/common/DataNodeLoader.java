package common;

import com.codahale.metrics.MetricRegistry;
import config.ConfigLoader;
import org.apache.gossip.GossipMember;
import org.apache.gossip.GossipService;
import org.apache.gossip.GossipSettings;
import org.apache.gossip.RemoteGossipMember;
import sun.security.krb5.Config;

import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataNodeLoader {

    public static IDataNode dataNode;

    public static void main(String[] args) throws Exception {

        validate(args);
        ConfigLoader.init(args[0]);

        int nodeId = Integer.parseInt(args[1]);
        Commons.nodeId = nodeId;
        String nodeInfo = ConfigLoader.config.nodesMap.get(nodeId);
        String type = ConfigLoader.config.dhtType;
        boolean distributed = "distributed".equalsIgnoreCase(type);
        String[] ipPort = nodeInfo.split(":");
        String ip = ipPort[0];
        int port = Integer.parseInt(ipPort[1]);
        dataNode = Commons.loadDataNode(ConfigLoader.config, nodeId);
        ServerSocket server = new ServerSocket(port);
        ClientWorker w = new ClientWorker( dataNode);

        if(distributed){
            List<Integer> members = new ArrayList<>();
            Map<Integer, List<Integer>> gossipList = ConfigLoader.config.gossipList;
            for (Map.Entry<Integer, List<Integer>> e : gossipList.entrySet()) {
                if(e.getValue().contains(nodeId)){
                    System.out.println("Gossip Group found for the node!!" + e.getValue());
                    members = e.getValue();
                    break;
                }
            }
            List<GossipMember> startupMembers = new ArrayList<>();
            for (Integer member: members) {
                if(member != nodeId) {
                    String[] splits = ConfigLoader.config.nodesMap.get(member).split(":");
                    String port1 = Integer.toString(Integer.parseInt(splits[1])+1000);
                    URI uri = new URI("udp://" +  splits[0]+":"+port1);
                    startupMembers.add(new RemoteGossipMember("dht", uri, Integer.toString(member)));
                }
            }
            GossipSettings settings = new GossipSettings();
            settings.setGossipInterval(100);
            settings.setCleanupInterval(1000);
            settings.setWindowSize(100);
            settings.setPersistDataState(false);
            settings.setPersistRingState(false);
            settings.setConvictThreshold(2.00);
            String [] splits = ConfigLoader.config.nodesMap.get(nodeId).split(":");
            String port1 = Integer.toString(Integer.parseInt(splits[1])+1000);
            URI uri = new URI("udp://" + splits[0] + ":" + ( port1));
            GossipService gossipService = new GossipService("dht", uri, Integer.toString(nodeId) + "",
                    new HashMap<String, String>(), startupMembers, settings, null, new MetricRegistry());
            Commons.gossip = gossipService;
            gossipService.start();
            Thread gsThread = new GossipThread(dataNode);
            gsThread.start();
        }


        while(true) {
            Socket clientSocket = server.accept();
            w.run(clientSocket);
        }
    }

    private static  void validate(String[] args) throws Exception {

        if(args.length !=2 )
            throw new Exception("Please provide two arguments. \n 1) absolute path of config file. 2)Node Id ");
    }
}