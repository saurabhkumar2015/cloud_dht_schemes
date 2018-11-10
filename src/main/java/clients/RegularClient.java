package clients;

import config.ConfigLoader;
import config.DHTConfig;
import java.io.FileReader;

public class RegularClient {


    private static String scheme;
    private static String dhtType;

    public static void main(String[] args) throws Exception {

        if(args.length != 2) throw new Exception("Please specify Two arguments. \n 1) Config file absolute path \n 2) File containing list of files to write in DHT.");

        ConfigLoader.init(args[0]);
        DHTConfig config = ConfigLoader.config;
        initRegularClient(config);
        FileReader fileList = new FileReader(args[1]);

    }

    private static void initRegularClient(DHTConfig config) {
        scheme = config.scheme;
        dhtType = config.dhtType;

    }
}
