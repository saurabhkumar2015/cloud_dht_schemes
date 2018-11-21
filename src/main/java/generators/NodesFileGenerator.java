package generators;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;

public class NodesFileGenerator {

    public static void main(String[] args) throws Exception {
        if(args.length != 3) throw new Exception("Please specify Three arguments." +
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
        System.out.println("Total number of nodes is: "+ max);
        System.out.println("Number of nodes per ip is:"+ factor);
        int init = 0;
        int start = 1;
        int end =0;
        int port =50000;
        for ( String ip : ips) {
            port =50000;
            start = init*factor + 1;
            init++;
            end = start +factor;
            System.out.println("IP "+ ip + " range start is :"+ start + " end is "+ end);
            for (int i = start ;i < end;i++) {
                bf.write(i +","+ip.trim()+":"+port++);
                bf.newLine();
            }
        }
        start = end;
        while(start <= max) {
            bf.write(start++ +","+ips[ips.length-1].trim()+":"+port++);
            bf.newLine();
        }
        bf.flush();
        bf.close();
    }
}
