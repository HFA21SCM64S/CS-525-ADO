How to compile:
use command line, direct to the folder c files exists, type "make", and then type "./test_assign4"


initIndexManager:initialize the index manager
shutdownIndexManager: shut down the index manager

createBtree:take the name, the data type(int), and the maximum number of nodes (2), and create a new tree
openBtree: take the name of the tree and store it in the tree pointer
closeBtree: free the tree pointer
deleteBtree: remove the tree

getNumNodes: take the tree, calculate the number of nodes the tree has and store it in result
getNumEntries: take the tree, calculate the number of values the tree contains and store it in result
getKeyType: take the tree, and store the data type the tree has to result

findKey: take the tree, a value and find its RID and store the RID to result
insertKey: take the tree, a value and its RID, and insert the value and RID to the tree
deleteKey: take the tree, a value, find and delete the value and its RID in the tree
openTreeScan: take the tree, and create a new scanhandle for the tree
nextEntry: take the scanhandle and output RID in the ascending order of values
closeTreeScan: take the scanhandle and free its management data

additional struct:
Node: store the information about its mother, whether it is leaf, the RIDs and values or pointers to other nodes
TreeInfo: store the information about bufferpool, pagehandle, the root of the tree, the number of values the tree has, and the maximum number of values one node can have
