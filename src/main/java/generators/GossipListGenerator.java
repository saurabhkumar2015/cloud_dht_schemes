package generators;

public class GossipListGenerator {
    public static void main(String[] args) throws Exception {
        if(args.length!=3) throw new Exception("Please specify Three arguments." +
                "\n 1)Nodes.csv location"
                + "\n 2) IP value");
    }
}
