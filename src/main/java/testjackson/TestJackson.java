package testjackson;

import ceph.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import config.ConfigLoader;
import schemes.ElasticDHT.ERoutingTable;

import java.io.IOException;
import java.util.Queue;

public class TestJackson {

    public static void main(String[] args) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

        ConfigLoader.init("C:/cloud/config.conf");
        CephRoutingTable rt = CephRoutingTable.giveInstance();

        EntryPoint entryPoint = new EntryPoint();
        entryPoint.BootStrapCeph();

        String str = mapper.writeValueAsString(rt);
        System.out.println(str);
        CephRoutingTable rr = mapper.readValue(str, CephRoutingTable.class);
        System.out.println("\n\n\n\n" +rr);

        ERoutingTable r = new ERoutingTable();
        ERoutingTable.giveInstance().giveRoutingTable();

        str = mapper.writeValueAsString(r);
        System.out.println(str);
        System.out.println("\n\n\n\n\nDEserialized::");
        ERoutingTable rr1 = mapper.readValue(str, ERoutingTable.class);
        System.out.println(rr1);


    }
}
