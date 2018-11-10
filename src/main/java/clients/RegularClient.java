package clients;

import common.IRoutingTable;
import config.ConfigLoader;
import config.DHTConfig;

import java.io.BufferedReader;
import java.io.FileReader;

public class RegularClient {


    private static String scheme;
    private static String dhtType;
    private static IRoutingTable routingTable;

    public static void main(String[] args) throws Exception {

        if(args.length != 2) throw new Exception("Please specify Two arguments. \n 1) Config file absolute path \n 2) File containing list of files to write in DHT.");

        ConfigLoader.init(args[0]);
        DHTConfig config = ConfigLoader.config;
        initRegularClient(config);
        FileReader f = new FileReader(args[1]);
        BufferedReader bf = new BufferedReader(f);
        String line = bf.readLine();

        while(line != null && line.length() != 0) {
            String [] splits = line.split("]");
            if(splits.length > 1 && splits[1].trim().length() > 0) {
                String fileName = splits[1].trim();

            }
            line = bf.readLine();
        }

    }

    private static void initRegularClient(DHTConfig config) {
        scheme = config.scheme;
        dhtType = config.dhtType;

    }
}
