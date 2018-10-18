import datanode.DataNode;

import java.util.*;

public class NSuccessorsReplicationSimulator extends ReplicationSimulator{

    private static int numReplicas =5;

    public static void main(String[] args) {

        //Simulate Add Files
        Scanner sc = new Scanner(System.in);
        List<DataNode> datanodes = getDataNodeList(20);

        Map<Integer, List<Integer>> inputMap = new HashMap<Integer, List<Integer>>();
        inputMap.put(4, Arrays.asList(90,91,92,93));
        inputMap.put(5, Arrays.asList(100,101,102,104));
        inputMap.put(7, Arrays.asList(105,106,107,108));

        System.out.println("Let us add file hash indexes in nodeId " + inputMap.keySet() );
        addFilesWithRingStrategy(datanodes, inputMap, numReplicas);
        printTheCurrentState(datanodes);
        System.out.println("Type a character to proceed ahead.");
        String response = sc.next();

        //Simulate Node Addition
        addNodeInRingStrategy(datanodes, 5, 21, 100,103);

        //Simulate Node Removal

        //Simulate Node Load Change

    }

    private static void addNodeInRingStrategy(List<DataNode> datanodes, int pos, int nodeId, int startRange, int endRange) {

        DataNode dn = new DataNode(nodeId);
        datanodes.add(pos, dn);
        DataNode successor  = datanodes.get(pos+1);



    }

    private static void addFilesWithRingStrategy(List<DataNode> datanodes,
                                                 Map<Integer,List<Integer>> inputMap,
                                                 int numReplicas) {


        for (Map.Entry<Integer, List<Integer>> e : inputMap.entrySet()) {
         int nodeId = e.getKey();
         int primaryPos = 0;
         for(int i =0 ; i < datanodes.size();i++) {
             if(nodeId == datanodes.get(i).nodeId){
                 primaryPos = i;
                 break;
             }
         }

         for(Integer hashIndex : e.getValue()) {
             for(int i=0; i< numReplicas;i++) {
                 HashMap<Integer,Integer> newFiles = new HashMap<Integer, Integer>();
                 newFiles.put(hashIndex,i+1);
                 datanodes.get(primaryPos+i).addNewFiles(newFiles);
             }
         }
        }
    }
}
