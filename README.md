"# cloud_dht_schemes" 

DHT schemes: Ring, Elastic, Ceph
DHT architecture type: Centralized, Distributed


Doc link: https://docs.google.com/document/d/1zJuuDiHpp24EAniAqsfeKmsXrEXuPTuteHOf3zakkEc/edit


1) Command to start datanode
java -cp schemes-1.0-SNAPSHOT-shaded.jar common.DataNodeLoader /home/config.conf 1

2) command to start control client
java -cp schemes-1.0-SNAPSHOT-shaded.jar clients.ControlClient /home/config.conf

3) command to start regular client
java -cp schemes-1.0-SNAPSHOT-shaded.jar clients.RegularClient /home/config.conf /home/full2.txt

4) Command to start all data nodes on machine
java -cp schemes-1.0-SNAPSHOT-shaded.jar datanode.DataNodeStart schemes-1.0-SNAPSHOT-shaded.jar /home/comfog.conf /home/full2.txt /home/nodes.csv 192.168.0.123

5) Command to kill all data nodes on machine

6) Command to start proxy server
java -cp schemes-1.0-SNAPSHOT-shaded.jar proxy.ProxyServer /home/vm_config.csv

7) Universal Test case
java -cp schemes-1.0-SNAPSHOT-shaded.jar test.UniversalDHTTest C:\cloud\config.conf C:\cloud\directories\full2.txt

8)Nodes.csv Config creator
java -cp schemes-1.0-SNAPSHOT-shaded.jar generators.NodesFileGenerator C:\cloud\vm_nodes1.csv C:\cloud\ips.txt 10000

9) Gossip list generetor
java -cp schemes-1.0-SNAPSHOT-shaded.jar generators.NodesFileGenerator C:\cloud\nodes.csv C:\cloud\gossip.txt 20 5

Command to compile::
mvn install shade:shade

jar name:
schemes-1.0-SNAPSHOT-shaded.jar