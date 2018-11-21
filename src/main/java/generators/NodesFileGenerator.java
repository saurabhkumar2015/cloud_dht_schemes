package generators;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;

public class NodesFileGenerator {

    public static void main(String[] args) throws Exception {
        if(args.length != 2) throw new Exception("Please specify Four arguments." +
                "\n 1)Nodes.csv location"
                +"\n 2)ips file location"
                +"\n 3)number of nodes");
        FileWriter f = new FileWriter(args[0]);
        BufferedWriter bf = new BufferedWriter(f);

        int max = Integer.parseInt(args[2]);
        FileReader f1 = new FileReader(args[1]);
        BufferedReader bf1 = new BufferedReader(f1);

        String line = bf1.readLine();
        String []ips = line.split(",");
        bf1.close();

        int factor = max/ips.length;
        int start = 1;
        for (String ip : ips) {
            int port =50000;
            int end = start +factor;
            for (int i =start ;i < end;i++) {
                bf.write(start++ +","+ip.trim()+":"+port++);
                bf.newLine();
            }
            while(start <= max) {
                bf.write(start++ +","+ip.trim()+":"+port++);
                bf.newLine();
            }
        }
        bf.flush();
        bf.close();
    }
}
