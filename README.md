"# cloud_dht_schemes" 

DHT schemes: Ring, Elastic, Ceph
DHT architecture type: Centralized, Distributed


Doc link: https://docs.google.com/document/d/1zJuuDiHpp24EAniAqsfeKmsXrEXuPTuteHOf3zakkEc/edit


1) Command to start datanode
java -cp schemes-1.0-SNAPSHOT-jar-with-dependencies.jar common.DataNodeLoader /home/config.conf 1

2) command to start control client
java -cp schemes-1.0-SNAPSHOT-jar-with-dependencies.jar clients.ControlClient /home/config.conf

3) command to start regular client
java -cp schemes-1.0-SNAPSHOT-jar-with-dependencies.jar clients.RegularClient /home/config.conf /home/full2.txt

4) Command to start all data nodes on machine

5) Command to kill all data nodes on machine

6) Command to start proxy server

java -cp schemes-1.0-SNAPSHOT-jar-with-dependencies.jar proxy.ProxyServer /home/vm_config.csv 
