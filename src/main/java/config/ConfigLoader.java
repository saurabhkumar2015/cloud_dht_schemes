package config;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
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
        if(!config.verbose.equalsIgnoreCase("error")) {
            System.out.println("Config is::" + config.toString());
            System.out.println("DHT Config Loaded Successfully!!!!");
        }
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
}
