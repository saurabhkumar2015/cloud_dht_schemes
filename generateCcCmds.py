# This script generates a file with list of commands 
# to add a node or to do load balance or to delete a node
# for a given range of node ids
# Node id and load factors are picked randomly

import json
import sys
from pprint import pprint
from random import randint
from random import *

fileGenerated = "cc_commands.txt"
configFile = "config.conf"

#print usage
if(len(sys.argv)==1):
	print("Usage:")
	print("python generateCcCmds.py <numberOfNodes>")
	print("\t nodeIds are taken from the end")
	print("\t numberOfNodes should be with in the range of nodeIdEnd mentioned in "+configFile+" file")
	print("\n")
	print("python generateCcCmds.py <numberOfNodes> <startingNodeId>")
	print("\t nodeIds are taken from the startingNodeId")
	sys.exit(0)

with open(configFile) as f:
	data = json.load(f)
#pprint(data['nodeIdEnd'])
nodeIdEnd = data['nodeIdEnd']
rangeOfIds = int(sys.argv[1])

fd = open(fileGenerated,"w")
if(len(sys.argv)>2):
	startRange = int(sys.argv[2])
	endRange = startRange + rangeOfIds-1
else:
  startRange = nodeIdEnd-rangeOfIds+1
  endRange = nodeIdEnd

#print("startRange:", startRange)
#print("endRange:", endRange)
  
'''  
for nId in range(startRange,endRange):
  fd.write("D,"+str(nId)+"\n")
  fd.write("A,"+str(nId)+"\n")
  fd.write("L,"+str(nId)+",0.5\n")
'''  

#Making randomized
nodeIdSet = set()  
total = endRange - startRange +1
#print("total:",total)
while(len(nodeIdSet)<total):
	#node ids are picked randomly
	nId = randint(startRange,endRange)
	if nId not in nodeIdSet:
		nodeIdSet.add(nId)
		#Seq operations: DeleteNode/AddNode/LoadBalance
		fd.write("D,"+str(nId)+"\n")
		fd.write("A,"+str(nId)+"\n")
		load = round(uniform(0.1,0.9), 2)
		fd.write("L,"+str(nId)+","+str(load)+"\n")
		
		'''	
		#This randomised method wont work if we delete the node first and load balance later
		optionList = [1,2,3]
		#actions are shuffled
		shuffle(optionList)
		#file write
		for option in optionList:
			if option == 1:
				fd.write("D,"+str(nId)+"\n")
			elif option == 2:
				fd.write("A,"+str(nId)+"\n")
			elif option == 3:
				load = round(uniform(0.1,0.9), 2)
				fd.write("L,"+str(nId)+","+str(load)+"\n")
		'''	
print("File "+fileGenerated+" generated successfully")

fd.close()
