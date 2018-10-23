import datanode.DataNode;
import datanode.FileRequest;

import java.util.*;

public class NSuccessorsReplicationSimulator extends ReplicationSimulator{

    private static int numReplicas =5;

    public static void main(String[] args) {

        //Simulate Add Files
        Scanner sc = new Scanner(System.in);
        List<DataNode> datanodes = getDataNodeList(12);

        Map<Integer, List<Integer>> inputMap = new HashMap<Integer, List<Integer>>();
        inputMap.put(4, Arrays.asList(90,91,92,93));
        inputMap.put(5, Arrays.asList(100,101,103,104));
        inputMap.put(7, Arrays.asList(106,107,108,109));
        inputMap.put(8, Arrays.asList(110,111));

        System.out.println("Let us add file hash indexes in nodeId " + inputMap.keySet() );
        addFilesWithRingStrategy(datanodes, inputMap, numReplicas);
        printTheCurrentState(datanodes);

        //Simulate Node Addition
        System.out.println("Type a character to proceed ahead with node addition. node id 21");
        String response = sc.next();
        addNodeInRingStrategy(datanodes, 5, 21, 100,103);
        printTheCurrentState(datanodes);

        System.out.println("Type a character to proceed ahead with another node addition nodeid 22");
        response = sc.next();
        addNodeInRingStrategy(datanodes, 9, 22, 110,110);
        printTheCurrentState(datanodes);

        System.out.println("Type a character to proceed ahead with another node addition nodeid 22");
        response = sc.next();
        addNodeInRingStrategy(datanodes, 12, 23, 114,115);
        printTheCurrentState(datanodes);



        //Simulate Node Removal
        System.out.println("Type a character to proceed ahead with node removal of nodeid 10");
        response = sc.next();
        removeNodeInRingStrategy(datanodes, 7);
        printTheCurrentState(datanodes);


        //Simulate Node Load Change

    }

    private static void removeNodeInRingStrategy(List<DataNode> datanodes, int i) {

        //get position of failed node.
        int pos =0;
        int j = 0;
        for (DataNode dn : datanodes) {
            if(dn.nodeId == i) {
                pos = j;
                break;
            }
            j++;
        }
        DataNode predecessor = datanodes.get(pos-1);
        DataNode successor = datanodes.get(pos-1);
    }

    /**
     * @param datanodes
     * @param pos
     * @param nodeId
     * @param startRange
     * @param endRange
     */
    private static void addNodeInRingStrategy(List<DataNode> datanodes, int pos, int nodeId, int startRange, int endRange) {

        DataNode dn = new DataNode(nodeId);
        datanodes.add(pos-1, dn);
        DataNode successor  = datanodes.get(pos);

        // Remove and get file indexes from all nodes for which the new node will have the primary file.
        FileRequest primary = new FileRequest();
        primary.startRange = startRange;
        primary.endRange = endRange;
        primary.replicaId = 1;
        primary.newReplicaId = 2;

        Map<Integer, FileRequest> nthReplicaMap = new HashMap<Integer, FileRequest>();
        for(Map.Entry<Integer, Set<Integer>> e: successor.storeMap.entrySet()){
            if(e.getKey() >= startRange && e.getKey() <= endRange ) continue;
            //Map<ReplicaId , FileRequest>
            for(Integer i : e.getValue()){
                if(i !=1) {
                    FileRequest fileRequest = nthReplicaMap.get(i);
                    if(fileRequest == null) {
                        fileRequest = new FileRequest();
                        fileRequest.hashIndexes = new ArrayList<Integer>();
                        fileRequest.hashIndexes.add(e.getKey());
                        fileRequest.replicaId = i;
                        if ( i == numReplicas) fileRequest.newReplicaId = -1;
                        else fileRequest.newReplicaId = i+1;
                        nthReplicaMap.put(i, fileRequest);
                    }
                    else fileRequest.hashIndexes.add(e.getKey());
                }
            }
        }

        List<FileRequest> requests = new ArrayList<FileRequest>(nthReplicaMap.values());
        requests.add(primary);
        Map<Integer, List<Integer>> successorIndexes = successor.getAndEditFiles(requests);

        // Create primary Map for our dataNode
        Map<Integer,Integer> newMap = new HashMap<Integer, Integer>();
        for(Map.Entry<Integer, List<Integer>> e : successorIndexes.entrySet()) {
            for(Integer i :e.getValue()) newMap.put(i,e.getKey());
        }
        dn.addNewFiles(newMap); // All primary files added to our new data nodes.

        for (int i=pos+1; i < pos + numReplicas -1; i++) {
            FileRequest shiftRequest = new FileRequest();
            shiftRequest.startRange = startRange;
            shiftRequest.endRange = endRange;
            shiftRequest.replicaId = i - pos +1;
            shiftRequest.newReplicaId = shiftRequest.replicaId +1;

            List<FileRequest> fr = new ArrayList<FileRequest>();
            fr.add(shiftRequest);

            for ( Map.Entry<Integer, FileRequest> e1 :nthReplicaMap.entrySet()) {
                if(e1.getKey() + i - pos <= numReplicas) {
                    FileRequest request = e1.getValue();
                    FileRequest shiftRequest1 = new FileRequest();
                    ArrayList<Integer> hashIndexes = new ArrayList<Integer>(request.hashIndexes);
                    shiftRequest1.hashIndexes = hashIndexes;
                    shiftRequest1.replicaId = request.replicaId + i - pos;
                    if (shiftRequest1.replicaId == numReplicas) {
                        shiftRequest1.newReplicaId = -1;
                    }
                    else shiftRequest1.newReplicaId = shiftRequest1.replicaId + 1;
                    fr.add(shiftRequest1);
                }
            }

            datanodes.get(i%datanodes.size()).editFiles(fr);
        }

        Map<Integer,Integer> lastMap = new HashMap<Integer, Integer>();
        for (Integer index : successorIndexes.get(1)) {
            lastMap.put(index, numReplicas);
        }
        datanodes.get((pos+numReplicas-1)%datanodes.size()).removeFiles(lastMap);
    }

    /**
     * @param datanodes
     * @param inputMap
     * @param numReplicas
     */
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
                    datanodes.get((primaryPos+i)%datanodes.size()).addNewFiles(newFiles);
                }
            }
        }
    }
}
