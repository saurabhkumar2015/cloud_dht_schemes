package generators;

import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;

public class GossipListGenerator {
    public static void main(String[] args) throws Exception {
        if(args.length!=4) throw new Exception("Please specify Four arguments." +
                "\n 1)Nodes.csv location"
                +"\n 2)output location"
                + "\n 3) max Size of gossip"
                + "\n 4)overlap size");

        Integer max = Integer.parseInt(args[2]);
        Integer overlap = Integer.parseInt(args[3]);
        FileReader f = new FileReader(args[0]);
        BufferedReader bf = new BufferedReader(f);
        String line = bf.readLine();

        FileWriter out = new FileWriter(args[1]);
        BufferedWriter bfo = new BufferedWriter(out);

        int gossipList = 1;
        List<Integer> nodesIds = new ArrayList<>();
        while(line != null) {
            if (line.trim().length() > 0)
                nodesIds.add(Integer.parseInt(line.split(",")[0]));
            line = bf.readLine();
        }

        for (int i=0 ; i <= nodesIds.size()/max;i++) {
            int start = Math.max(max*i -overlap, 0);
            int end = Math.min(start+max, nodesIds.size());
            if(nodesIds.size() - end <= overlap) {
                end = nodesIds.size();
            }
            List<Integer> integers = nodesIds.subList(start, end);
            bfo.write(Integer.toString(gossipList)+ ": \"" + StringUtils.join(integers,',')+"\"");
            bfo.newLine();
            gossipList++;
        }
        bfo.flush();
        bf.close();
        bfo.close();
    }
}
