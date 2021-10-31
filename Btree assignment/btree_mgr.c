#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include "buffer_mgr.h"  // Functionality of buffer manager
#include "storage_mgr.h" // Functionality of storage manager

#include "dberror.h"
#include "btree_mgr.h"  // Function prototypes for btree_mgr
#include "tables.h"
#include "expr.h"


#include "btree_mgr.h"
#include "storage_mgr.h"

// Set last page to 0
int lastPage=0;

// Initialize RID
const RID INIT_RID={-1,-1};


// Initialize scan count to 0
int scanCount=0;


// Create the node struct
typedef struct Node
{
	int mother;  // Parent of the node 
	bool leaf;   // Leaf or not
	RID left;     // Left
	int value1;   // Value 1_
	RID mid;      // Mid 
	int value2;   // Value2 of the node
	RID right;
}Node;




typedef struct TreeInfo


{
	BM_BufferPool *bm;  // POinter to BUfferPool

	BM_PageHandle *page; // Pointer to BM PageHandle
	int root;   // POinter to root
	int globalCount;//number of keys
	int maxCount;
}TreeInfo;



RC initIndexManager (void *mgmtData)
{
	printf("Calling function initIndexManager \n");
	return RC_OK;
}

RC shutdownIndexManager ()
{
	printf("Calling function shutdownIndexManager\n");
	return RC_OK;
}

RC createBtree (char *idxId, DataType keyType, int n)
{
	printf("Calling function createBtree \n");
	createPageFile(idxId);

	SM_FileHandle fh;
	openPageFile(idxId, &fh);
	ensureCapacity(1, &fh);

	SM_PageHandle ph=malloc(PAGE_SIZE*sizeof(char));

	//index manager only handles integer values
	if(keyType!=DT_INT)
	{
		printf("The values are not integers!!!\n");
		return RC_RM_UNKOWN_DATATYPE;
	}

	
	*((int *)ph)=n;

	writeCurrentBlock(&fh,ph);

	closePageFile(&fh);

	return RC_OK;
}

RC openBtree (BTreeHandle **tree, char *idxId)
{
	printf("Calling function openBtree\n");
	TreeInfo* trInfo=malloc(sizeof(TreeInfo));
	trInfo->bm=MAKE_POOL();
	trInfo->page=MAKE_PAGE_HANDLE();
	trInfo->globalCount=0;
	trInfo->root=0;

	initBufferPool(trInfo->bm,idxId, 10, RS_FIFO, NULL);// Assume max  10_ pages

	pinPage(trInfo->bm, trInfo->page,1);



	BTreeHandle* treeTemp;
	treeTemp=(BTreeHandle*)malloc(sizeof(BTreeHandle));
	treeTemp->keyType=DT_INT;
	trInfo->maxCount=*((int *)trInfo->page->data);
	printf("max %d\n", trInfo->maxCount);
	treeTemp->idxId=idxId;
	treeTemp->mgmtData=trInfo;

	*tree=treeTemp;
	unpinPage(trInfo->bm, trInfo->page);
 
	return RC_OK;
}

RC closeBtree (BTreeHandle *tree)
{
	printf("Calling function closeBtree\n");
	lastPage=0;
	scanCount=0;
	free(tree);
	free(((TreeInfo* )(tree->mgmtData))->page);
	shutdownBufferPool( ((TreeInfo* )(tree->mgmtData))->bm );

	return RC_OK;
}

RC deleteBtree (char *idxId)
{
	printf("Calling function deleteBtree\n");
	if(remove(idxId)!=0)		//check whether the name exists
		return RC_FILE_NOT_FOUND;
	return RC_OK;

}


// Get information about the BTree



// This function returns the numbet of nodes in a BTree
RC getNumNodes (BTreeHandle *tree, int *result){

    // Get the lastPage. Page count needs to be incremented by 1_
      int  no_of_nodes=lastPage+1;
	(*result)= no_of_nodes; // store the number_of_nodes in the result
	// Return after storing the no_of_nodes in the variable result
	// Normal return is RC_OK
	return RC_OK;
}


// THis function returns the number of Entreies in a BTree

RC getNumEntries (BTreeHandle *tree, int *result) {


       // From mgmt struct, get TreeInfo struct
	TreeInfo* tree_info_ptr=(TreeInfo*) ((*tree).mgmtData);  // get mgmtData Member
	// From TreeInfo struct, get the value of globalCount member of the TreeInfo Struct
	int no_of_entries=(*tree_info_ptr).globalCount;

	// Store no_of_entries in result pointer
	(*result)=no_of_entries;
	// Return the normal termination RC_OK
	return RC_OK;
}



// This gunction returns the key type
RC getKeyType (BTreeHandle *tree, DataType *result){
	

// Return the DataType of the key

        (*result) = DT_INT; // DataTYpe of key
	
	return RC_OK;
}





// This function finds a key in the BTree
RC findKey (BTreeHandle *tree, Value *key, RID *result){

       // Get the pointer to mgmt and cast it to TreeInfo
	TreeInfo* tree_info_ptr=(TreeInfo*) ((*tree).mgmtData); 

	// Which key to find
	int key_to_find=key->v.intV;

	// Vaiable to hold the pointer to node
	Node* tree_node;

	// Is the key present
	bool is_present=false;


	 // Index Value Odf Interest
        int index_value;


	// Increment to reach to the desired node
	
	int bool_size=sizeof(bool);

	// Go over all one by one
	for(index_value=1;index_value<lastPage+1;index_value=index_value+1)
	{
		// First move the page from file to the memory
		pinPage(tree_info_ptr->bm, tree_info_ptr->page, index_value);

	        // POinter to the starting address
	        Node* nodetree_node_start_address=((Node*)tree_info_ptr->page->data);	
		

		tree_node=nodetree_node_start_address+bool_size;
		
		// val_1
		int val_1=tree_node->value1;
		
		//val_2
		int val_2=tree_node->value2;
                

		// if key_to_find is val_1
		//
		//
		if(key_to_find==val_1)
		{
			is_present=true;
			(*result)=(*tree_node).left;
			break;
		}
		
		
		// if key_to_find is val_2
		//
		//
		if(key_to_find==val_2)
		{
			is_present=true;
			(*result)=(*tree_node).mid;
			break;
		}
	
	    // Once you are done checking, Unpin the page
	    //
	    //
		unpinPage(tree_info_ptr->bm, tree_info_ptr->page);
	}


	// If key was not present
	if(is_present==false){
	   
	   return RC_IM_KEY_NOT_FOUND;
	}


	// If key was present, do normal termination
	//
	//
	return RC_OK;
}


// This function inserts a key in the BTree
//
//
//


RC insertKey (BTreeHandle *tree, Value *key, RID rid){
	
	
	TreeInfo* tree_info_ptr=(TreeInfo*) (tree->mgmtData);
        
        int bool_size=sizeof(bool);	
	Node* tree_node;


	if(lastPage==0)
	{

		// If this is page 0 and we are inserting first value 
		
		// LatPage is set to 1
		lastPage=1;


		// Root is also set to 1_ because this is the first page
		tree_info_ptr->root=1;  // THis is the root
                
		// Pin the page from file to memory
		pinPage(tree_info_ptr->bm, tree_info_ptr->page, lastPage);
		
		// Mark the page Dirty
		markDirty(tree_info_ptr->bm, tree_info_ptr->page);
		
		//Now change the flag to show that this page is having some value
		//
		void* data_ptr=tree_info_ptr->page->data; // Catch all by casting to void*
		*((bool*)data_ptr)=false;
		//Now construct the node
		//
		//

		// Get to the appropriate address
		tree_node=((Node*)data_ptr)+bool_size;
		// Set mother to -1
		tree_node->mother=-1;

		// Set leaf node to true
		tree_node->leaf=true;

		// Set left node
		tree_node->left=rid; // Set left node

		// SEt value_1
		tree_node->value1=key->v.intV; // set value 1
		// Set the mid
		tree_node->mid=INIT_RID;  // set the mid
		
		
		// Now aet value 2
		tree_node->value2=-1;


		// set the right node
		tree_node->right=INIT_RID;  // set the right node
                
		//unpin page: It is already marked dirty
		unpinPage(tree_info_ptr->bm, tree_info_ptr->page);
	}
	else
	{
                // Pinthe page
		pinPage(tree_info_ptr->bm, tree_info_ptr->page, lastPage);
		// Mark the page as Dirty
		markDirty(tree_info_ptr->bm, tree_info_ptr->page);


		//if the page is full
		//
		//
	    void* data_ptr_2=tree_info_ptr->page->data;
		if((*(bool*)data_ptr_2)==true)
		{

			// Increment lastPage by 1_
			lastPage=lastPage+1;

			//Unpin the page
			//

			unpinPage(tree_info_ptr->bm, tree_info_ptr->page);
			
			
			pinPage(tree_info_ptr->bm, tree_info_ptr->page, lastPage);

			
			*(bool*)tree_info_ptr->page->data=false;
			//Now construct the node
			//

			tree_node=((Node*)tree_info_ptr->page->data)+bool_size;
			
			// Increment mother
			tree_node->mother=-1;


			// set leaf to true
			tree_node->leaf=true;

			// SEt left to rid
			tree_node->left=rid;

			// Insert value1
			tree_node->value1=key->v.intV;
			tree_node->mid=INIT_RID;

			// INsert value2
			tree_node->value2=-1;

			// Update right node
			tree_node->right=INIT_RID;
                        
			// unpin the page
			unpinPage(tree_info_ptr->bm, tree_info_ptr->page);
		}
		else // If page has existing entries  but still has spave
		{
			// Get the right node
			tree_node=(Node*)tree_info_ptr->page->data+bool_size;
			
			// SERt the mid member of the treenode struct
			tree_node->mid=rid;

			// Insert value2 in the node
			tree_node->value2=key->v.intV;

			// SEt the status to true
			*(bool*)tree_info_ptr->page->data=true;
                       

			// UNpin the page after insertion
			unpinPage(tree_info_ptr->bm, tree_info_ptr->page);
		}
	}


      // INcrement the globalCount by 1 after insertion
	(tree_info_ptr->globalCount)++;

	// Return normal termination
	return RC_OK;
}

RC deleteKey (BTreeHandle *tree, Value *key)
{
	TreeInfo* trInfo=(TreeInfo*) (tree->mgmtData);
	int index, findKey=key->v.intV;
	Node* node;
	bool find=false;
	int valueNum=0;
	RID moveRID;
	int moveValue;

	for(index=1;index<=lastPage;index++)
	{
		pinPage(trInfo->bm, trInfo->page, index);
		markDirty(trInfo->bm, trInfo->page);
		node=(Node*)trInfo->page->data+sizeof(bool);
		int v1=node->value1;
		int v2=node->value2;
//		printf("key:%d, v1:%d, v2:%d\n",findKey,v1,v2 );
		if(findKey==v1)
		{
			find=true;
			valueNum=1;
			break;
		}
		if(findKey==v2)
		{
			find=true;
			valueNum=2;
			break;
		}
		unpinPage(trInfo->bm, trInfo->page);
	}

	if(find==false)
		return RC_IM_KEY_NOT_FOUND;
	else//if this value exists
	{
		pinPage(trInfo->bm, trInfo->page, lastPage);
		markDirty(trInfo->bm, trInfo->page);

		if(index==lastPage)
		{//if we are deleting a value in the last page
			node=(Node*)trInfo->page->data+sizeof(bool);
			if(valueNum==2)//if we are deleting the last value
			{			
				node->mid=INIT_RID;
				node->value2=-1;
				*(bool*)trInfo->page->data=false;			
			}
			else
			{
				if((*(bool*)trInfo->page->data)==true)//if we are deleting the second last value
				{
					moveRID=node->mid;
					node->left=moveRID;
					moveValue=node->value2;
					node->value1=moveValue;
					node->mid=INIT_RID;
					node->value2=-1;
					*(bool*)trInfo->page->data=false;
				}
				else//if we are deleting the last value in the first position
				{
					node->left=INIT_RID;
					node->value1=-1;
					lastPage--;
				}
			}
			unpinPage(trInfo->bm, trInfo->page);//unpin the last page
		}
		else
		{//if we are deleting a value not in the last page
			//if the last page is full
			if((*(bool*)trInfo->page->data)==true)
			{
				//set the page to be not full
				*(bool*)trInfo->page->data=false;
				//create the node
				node=(Node*)trInfo->page->data+sizeof(bool);
				moveRID=node->mid;
				moveValue=node->value2;

				node->mid=INIT_RID;
				node->value2=-1;

				unpinPage(trInfo->bm, trInfo->page);

				pinPage(trInfo->bm, trInfo->page, index);
				markDirty(trInfo->bm, trInfo->page);
				if(valueNum==1)
				{
					node=(Node*)trInfo->page->data+sizeof(bool);
					node->left=moveRID;
					node->value1=moveValue;
					unpinPage(trInfo->bm, trInfo->page);
				}
				else
				{
					node=(Node*)trInfo->page->data+sizeof(bool);
					node->mid=moveRID;
					node->value2=moveValue;
					unpinPage(trInfo->bm, trInfo->page);
				}
			}
			else//if the last page is not full
			{
				node=(Node*)trInfo->page->data+sizeof(bool);
				moveRID=node->left;
				moveValue=node->value1;
				node->left=INIT_RID;
				node->value1=-1;
				lastPage--;

				unpinPage(trInfo->bm, trInfo->page);//unpin the last page

				pinPage(trInfo->bm, trInfo->page, index);
				markDirty(trInfo->bm, trInfo->page);
				if(valueNum==1)
				{
					node=(Node*)trInfo->page->data+sizeof(bool);
					node->left=moveRID;
					node->value1=moveValue;
					unpinPage(trInfo->bm, trInfo->page);
				}
				else
				{
					node=(Node*)trInfo->page->data+sizeof(bool);
					node->mid=moveRID;
					node->value2=moveValue;
					unpinPage(trInfo->bm, trInfo->page);
				}
			}
		}
		(trInfo->globalCount)--;		//value number decreases by 1
	}

	return RC_OK;
}

RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle)
{
	printf("We are opening tree scan!!!\n");
	TreeInfo* trInfo=(TreeInfo*) (tree->mgmtData);
	Node *node;
	int *values;
	int index,i=0,j=0,temp1,temp2,min;
	values=(int*)malloc(sizeof(int)*(trInfo->globalCount));

	for(index=1;index<=lastPage;index++)
	{
		pinPage(trInfo->bm, trInfo->page, index);
		node=(Node*)trInfo->page->data+sizeof(bool);
		int v1=node->value1;
		int v2=node->value2;
//		printf("v1:%d, v2:%d\n",v1,v2 );
		if(v1!=-1)
		{
			values[i]=node->value1;
			i++;
		}
		if(v2!=-1)
		{
			values[i]=node->value2;
			i++;
		}
		unpinPage(trInfo->bm, trInfo->page);
	}

	//sort
	for(i=0;i<(trInfo->globalCount);i++)
	{
		min=i;
		for(j=i+1;j<(trInfo->globalCount);j++)
		{
			if(values[min]>values[j])
				min=j;
		}
		temp1=values[min];
		temp2=values[i];
		values[min]=temp2;
		values[i]=temp1;
	}
/*
	for(i=0;i<(trInfo->globalCount);i++)
	{
		printf("no.%d:%d\n",i,values[i] );
	}
*/
	BT_ScanHandle *handleTemp;
	handleTemp=(BT_ScanHandle*)malloc(sizeof(BT_ScanHandle));
	handleTemp->tree=tree;
	handleTemp->mgmtData=values;
	*handle=handleTemp;

	scanCount=0;

	return RC_OK;
}

RC nextEntry (BT_ScanHandle *handle, RID *result)
{
	printf("We are getting the next entry!!!\n");
	TreeInfo* trInfo=(TreeInfo*) (handle->tree->mgmtData);
	printf("%d\n", trInfo->globalCount);
	int* values=(int*)(handle->mgmtData);
	Value* vl;
	vl=(Value*)malloc(sizeof(Value));
	vl->dt=DT_INT;
	vl->v.intV=values[scanCount];
	RID* rslt;
	rslt=(RID*)malloc(sizeof(RID));

	if(scanCount==(trInfo->globalCount))
	{
		return RC_IM_NO_MORE_ENTRIES;
	}
	else
	{
		findKey (handle->tree, vl, rslt);
		scanCount++;
	}

	*result=*rslt;

	return RC_OK;
}

RC closeTreeScan (BT_ScanHandle *handle)
{
	printf("We are closing tree scan!!!\n");
	scanCount=0;
	free(handle->mgmtData);
	return RC_OK;
}

// ******************************************** debug and test functions *************************************
extern char *printTree (BTreeHandle *tree)
{
	return tree->idxId;
}
