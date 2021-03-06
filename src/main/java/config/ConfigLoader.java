package config;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Holds the config corresponding to the the DHT config file.
 * User needs to call the init method before accessing the config object.
 * Config object is only populated after the init method is called.
 */
public class ConfigLoader {

    private static ObjectMapper mapper = new ObjectMapper();

    public static DHTConfig config = new DHTConfig();

    public static void init(String configFile) throws IOException {

        Map map = getConfigMap(configFile);
        config.nodeIdStart = Integer.parseInt(map.get("nodeIdStart").toString());
        config.nodeIdEnd = Integer.parseInt(map.get("nodeIdEnd").toString());
        System.out.println("Node Id start end is ::" + config.nodeIdStart +"-"+ config.nodeIdEnd);
        config.proxyIp = map.get("proxyIp").toString();
        config.bucketSize = Integer.parseInt(map.get("bucketSize").toString());
        config.verbose  = map.get("verbose").toString();
        config.dhtType  = map.get("dhtType").toString();
        config.scheme  = map.get("scheme").toString();
        config.replicationFactor  = Byte.parseByte(map.get("replicationFactor").toString());
        config.cephMaxClusterSize  = Byte.parseByte(map.get("cephMaxClusterSize").toString());
        config.nodeMapLocation  = map.get("nodeMapLocation").toString();
        config.PlacementGroupMaxLimit  = Integer.parseInt(map.get("PlacementGroupMaxLimit").toString());
        config.nodesMap = getNodeMap(config.nodeMapLocation);
        config.resizeFactor = Integer.parseInt(map.get("resizeFactor").toString());
        config.gossipList = getGossipList((Map<String, String>) map.get("gossipList"));
       // config.sleepTime = Integer.parseInt(map.get("sleepTime").toString());
        config.sleepTime = Integer.parseInt(map.get("sleepTime").toString());
        config.gossipSleep = Integer.parseInt(map.get("gossipSleep").toString());
        config.commmandsFileLocations = map.get("commmandsFileLocations").toString();
        config.sleepBtwnCmds =  Integer.parseInt(map.get("sleepBtwnCmds").toString());
        config.logFileForWrite  = map.get("logFileForWrite").toString();
        config.logFileForCC  = map.get("logFileForCC").toString();
        if(!config.verbose.equalsIgnoreCase("error")) {
//            System.out.println("Config is::" + config.toString());
            System.out.println("DHT Config Loaded Successfully!!!!");
        }
    }

    private static Map<Integer,List<Integer>> getGossipList(Map<String, String> gossipList) {

        Map<Integer, List<Integer>> map = new HashMap<Integer, List<Integer>>();
        for(Map.Entry<String, String> e: gossipList.entrySet()) {
            Integer gossipId = Integer.parseInt(e.getKey());
            String[] ids = e.getValue().split(",");
            List<Integer> list = new ArrayList<Integer>();
            for(String id : ids) {
                list.add(Integer.parseInt(id));
            }
            map.put(gossipId, list);
        }
        return map;
        }

    private static Map getConfigMap(String configFile) throws IOException {

        File f = new File(configFile);
        return mapper.readValue(f, Map.class);
    }

    private static Map<Integer,String> getNodeMap(String nodeFile) throws IOException {

        FileReader f = new FileReader(nodeFile);
        BufferedReader bf = new BufferedReader(f);
        String line  =  bf.readLine();
        Map<Integer, String> map = new HashMap<Integer, String>();

        while(line != null && line.length() >0 ) {
            String [] arr = line.split(",");
            map.put(Integer.parseInt(arr[0]), arr[1]);
            line = bf.readLine();
        }
        return  map;
    }
    
    public static String GetNodeAddressFromNodeId(int nodeId)
    {
    	Map<Integer,String> nodeIdToAddress = config.nodesMap;
    	return nodeIdToAddress.get(nodeId);
    }
}
