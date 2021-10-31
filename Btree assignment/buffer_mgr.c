#include "buffer_mgr.h"
#include <stdio.h>
#include <stdlib.h>
#include "dberror.h"
#include "storage_mgr.h"


#define maxBufferPoolNum 10 // Maximum number for BufferPool
#define currentNum 0


RC ReplacementStra(BM_BufferPool *const bm, BM_PageHandle *const page,const PageNumber pageNum);
int currentBufferPoolNum=0;



//Variable to hold bufferpool information 




typedef struct BufferRecord{ 
    int bufferpoolNumber;
    
    // How many times a page has been read
    int readNum;

    // How many times a page has been written
    int writeNum; //write number
    int pageAge; //a buffer bool level counter
    
    // POlicy
    int lru_k;
    struct PageFrame *first;  // Pointer to the first PageFrame
    struct PageFrame  *last;  // Pinter to the last PageFrame
    
    // NUmber of pages
    int numPages;    // NUmber of Pages
    
} BufferRecord; 


BufferRecord bufferRecord[maxBufferPoolNum]; // Array of bufferRecord structs


typedef struct PageFrame{
    int frameNum; // Frame number
    int PageNum; // Page number
    bool dirty; // clean/dirty status of the page
    int lru_k;  // POlicy_1
    int clockwise;  // POlicy_2
    char *data;  //Data
   int pageTimeRecord; // Time metric for the page
    int fixCount; //fix Count for the page
    BM_PageHandle pageHandle;
    struct PageFrame *next; // Pointer to the next PageFrame

} PageFrame;






// Create an arry of pageFrame structs

SM_FileHandle *fh;  //Pointer to file handle
char *memPage;
PageFrame *PageFrameArray[100]; // Array of page Frame structs



// This function sets the BufferRecord to specified input
void setBufferRecord(BM_BufferPool *const bm){
     bufferRecord[currentNum].numPages=bm->numPages;
     bufferRecord[currentNum].first=PageFrameArray[0];
     bufferRecord[currentNum].last=PageFrameArray[bm->numPages-1];
}



// This function initializes the page frame


void initPageFrame(BM_BufferPool *const bm){
    
	
    PageFrame *PageFrameNode[bm->numPages];  //
    
    int loop_i;

    // Loop over all pages 
    for(loop_i=bm->numPages-1;loop_i>=0;loop_i--)
    {
        
	//Allocate an array for pageFrames
	PageFrameNode[loop_i]=(PageFrame *) malloc (sizeof(PageFrame));  
        
	
	PageFrameNode[loop_i]->pageHandle.pageNum=NO_PAGE;;
        
	printf("from inside the function initPageFrame, PageFrameNode[loop_i]->pageHandle.pageNum is: %d \n",PageFrameNode[loop_i]->pageHandle.pageNum); 
	
	PageFrameNode[loop_i]->frameNum=loop_i;


	// Initialize TimeREcord for the page
        PageFrameNode[loop_i]->pageTimeRecord =0;
        
	// Initialize dirty status of the page
	PageFrameNode[loop_i]->dirty=0;

	// Initialize policy
        PageFrameArray[loop_i]->lru_k;
	// Initialize fixCount
        PageFrameNode[loop_i]->fixCount=0; 
       PageFrameNode[loop_i]->data=bm->mgmtData+PAGE_SIZE*(loop_i);
        if(loop_i!=bm->numPages-1)
            {   
                PageFrameNode[loop_i]->next=PageFrameNode[loop_i+1]; //Address of Next page.
                
            }
        else 
            {
                PageFrameNode[loop_i]->next=NULL;
            }

    PageFrameArray[loop_i]=PageFrameNode[loop_i];
     }

      if(PageFrameArray!=NULL)
        {
        setBufferRecord(bm);
        }
       
    }

// Check for a specific page
PageFrame *checkTargetPage(BM_BufferPool *const bm, BM_PageHandle *const page)
{

// Get pointer to page frame
    PageFrame *pf;

// Assign it a buffer record
    pf=bufferRecord[currentNum].first;
    
    
    int loop_i;

// Go over all pages 1 by 1
for(loop_i = 0 ; loop_i<bm->numPages;loop_i++)
    {
    
	    if(PageFrameArray[loop_i]->pageHandle.pageNum==page->pageNum){

    		return PageFrameArray[loop_i];

	}
        
           }


    return NULL;
}

// sets the buffer
void setBuffer(BM_BufferPool *const bm, const char *const pageFileName, const int numPages, ReplacementStrategy strategy, void *stratData)
{
     char* buff = (char *)calloc(PAGE_SIZE*numPages,sizeof(char));
    // aet the mgmtData member
    bm->mgmtData=buff;
    
    // set the PageFile to PageFileName
    bm->pageFile = (char*)pageFileName;
    
    // set num Pages
    bm->numPages = numPages;
    
    // set the Strategy
    bm->strategy = strategy;
}

void initBufferRecord(int i, int numPages)    // initialze bufferRecord to hold all the bufferpool information
{
    char*cache = (char *)calloc(PAGE_SIZE*numPages,sizeof(char));

    // Initialize readNum
    bufferRecord[i].readNum=0;

    // Initialize write NUM
    bufferRecord[i].writeNum=0;

    // Initialize page Age
    bufferRecord[i].pageAge=0;

    // INitialize policy
    bufferRecord[i].lru_k=1;
    
  
}

RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,const int numPages, ReplacementStrategy strategy,void *stratData)
{
    
  initBufferRecord(currentBufferPoolNum,numPages);
    fh=(SM_FileHandle *)malloc(sizeof(SM_FileHandle));
   
    setBuffer(bm,pageFileName,numPages,strategy,stratData);
    
    // Open page file for reading/Writing
    openPageFile (bm->pageFile, fh);
    // Initialize pageFrame
    initPageFrame(bm);

    // Normal return
    return RC_OK;
}



// THis function returns PageFrameInfo
PageFrame *PageFrameInfo(const PageNumber pageNum)
{
     PageFrame *pf=(PageFrame *) malloc (sizeof(PageFrame));

    pf->dirty=0;
    
    pf->fixCount=1;
printf("pf->fixCount is: %d \n", pf->fixCount);
    pf->next=NULL;

    pf->pageHandle.pageNum=pageNum;

    // Return pf
    return pf;
}


// This function pins a page
RC pinPageFrame (BM_BufferPool *const bm, BM_PageHandle *const page,const PageNumber pageNum){
   
	
    PageFrame *pf;
    pf=bufferRecord[currentNum].first;
    int i;
    int check =0; 


page->pageNum=pageNum;  // Get the page number
    
     if(pf->pageHandle.pageNum==pageNum&&!(bufferRecord[currentNum].first==0))
        check=1;
 i=1;



 while(i<bm->numPages&&check==0)
    {
        pf=pf->next;
        if(pf->pageHandle.pageNum==pageNum)
            check=1;
        i++;
    }

    
    if(check==1)  
    {
        pf->fixCount++;
        page->data=pf->data; 
        return RC_OK;
    }

    // Switch based on the page replacement strategy
    switch(bm->strategy)            
            {

		// Page replacement strategy
                case RS_FIFO:  // Page replacement strategy FIFO
                   

		case RS_LRU:  // Replacement strategy LRU
                    bufferRecord[currentBufferPoolNum].pageAge++;

		    pf->pageTimeRecord=bufferRecord[currentBufferPoolNum].pageAge;
                case RS_LRU_K: // RS is RS_LRU_K:
                    pf->lru_k++;

		    if (bufferRecord[currentBufferPoolNum].lru_k==pf->lru_k)
                    bufferRecord[currentBufferPoolNum].pageAge++;
                    pf->pageTimeRecord=bufferRecord[currentBufferPoolNum].pageAge;

                case RS_CLOCK: // RS is RS_CLOCK
                    pf->clockwise=1;

            }
    
ReplacementStra(bm,page,pageNum);
}
 
RC ReplacementStra (BM_BufferPool *const bm, BM_PageHandle *const page,const PageNumber pageNum){
    PageFrame* pfPtr=bufferRecord[currentNum].first;
 
    PageFrame *newPageFrame;
    newPageFrame=PageFrameInfo(pageNum);

    PageFrame* pfSearch=bufferRecord[currentNum].first;
                 


    bufferRecord[currentNum].first=pfPtr->next;  
    
    
    newPageFrame->data=pfPtr->data;
                                                   
    // If page is dirty   
    if(pfPtr->dirty==1)                               
        {
            writeBlock(pfPtr->pageHandle.pageNum,fh, pfPtr->data);
            bufferRecord[currentBufferPoolNum].writeNum++;
        }
     newPageFrame->frameNum=pfPtr->frameNum;
    free(pfPtr);                                     
            
 
    
    
    readBlock(pageNum, fh, newPageFrame->data);
    page->data=newPageFrame->data;

    bufferRecord[currentBufferPoolNum].readNum++;
    bufferRecord[currentNum].last->next=newPageFrame;
    bufferRecord[currentNum].last=newPageFrame;
    PageFrameArray[newPageFrame->frameNum]=newPageFrame;
            


    return RC_OK;
}






//This function shuts down the buffer Pool

RC shutdownBufferPool(BM_BufferPool *const bm){
    int  result,i;
    PageFrame *pf;
    pf=bufferRecord[currentNum].first;
    result=forceFlushPool(bm);         //store data into memory
    if (result=RC_OK)
        {

       		setBuffer(bm,NULL,0,0,NULL);
          bufferRecord[currentBufferPoolNum].readNum=0;
       
	  bufferRecord[currentBufferPoolNum].writeNum=0;//free(bm->mgmtData);
        
	
	  closePageFile(fh);
                      for(i=0;i<bm->numPages;i++)
                      {
                                    if(pf->fixCount!=0){
                                    printf("write failed\n");
                                    return RC_WRITE_FAILED;
                                     }
                         pf=pf->next;
                        return RC_OK;
                      }
        
	
	}     
       else
       {

        return RC_WRITE_FAILED;
       
       }

    
}



// This function flushes the buffer pool
RC forceFlushPool(BM_BufferPool *const bm){     //function used to store data into memory
    PageFrame *pf;
    int result;
    pf=bufferRecord[currentNum].first;
    int i;
    openPageFile(bm->pageFile, fh);


    while(pf!=NULL) //loop to check dirty files
    {
        if(pf->dirty==true&&pf->fixCount==0)   //check file if it is dirty
        {
    
        result=writeBlock(pf->pageHandle.pageNum,fh, pf->data);
        bufferRecord[currentBufferPoolNum].writeNum++;
             pf->dirty=0;  //after storing set the dirty flag into 0
        }

        pf=pf->next;
    }
    printf("forced Flush \n");
    return RC_OK;
}

// Marks a page Dirty
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page){
    PageFrame *pf;
    pf=checkTargetPage(bm,page);
    if(pf!=NULL)
     { 
                    pf->dirty=1;
                    return RC_OK;
      }
      else
      {
        //Ensure that the page was marked dirty
            return RC_NO_SUCH_PAGE_IN_BUFF;
    }
}

// THis function forces a page
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page){
    PageFrame *pf;
    
    int result=-1;
    
    pf=checkTargetPage(bm,page);
    
    if(pf!=NULL) // If pf is not NULL
    
    {
    
	result=writeBlock(pf->pageHandle.pageNum,fh, pf->data);
    bufferRecord[currentBufferPoolNum].writeNum++;
    
    
    if (result!=0)
        {
            return RC_WRITE_FAILED;
        }
    
    return RC_OK;
     }
    
    else
      {
        return RC_WRITE_FAILED;
      }
}



// THis function pins a page
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page,const PageNumber pageNum){
    page->pageNum=pageNum;
  
            return pinPageFrame(bm,page,pageNum);
    

}


// THis function unpins a page
RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page){
    PageFrame *pf;
    int check;
    pf=checkTargetPage(bm,page);
    //check the PageFrame founded by check tragert page
    if(pf!=NULL)
       {
        check=(*(pf->data)==*(page->data));
        if (check!=1)
            printf("failed to unpin page\n");
        if (pf->fixCount>0)
            pf->fixCount-=1;
        return RC_OK;
     }
    else
         {
            printf("unpin page failed \n");
         return RC_UNPIN_ERROR;
         }
}


// Get the contents of the frame

PageNumber *getFrameContents (BM_BufferPool *const bm){
    
	
	
    PageFrame *pf;
    
    pf=bm->mgmtData;
    
    PageNumber *frameContent;
    
    frameContent = (int *)malloc(sizeof(int)*bm->numPages);
    
    int loop_i;
    
    
    for(loop_i = 0; loop_i < bm->numPages; loop_i=loop_i+1){
        
	    
	 frameContent[loop_i] = pf[loop_i].pageHandle.pageNum;
    }


    return frameContent;

    PageNumber (*arr)[bm->numPages];
    
    arr=calloc(bm->numPages,sizeof(PageNumber));
     
    for ( loop_i =bufferRecord[currentNum].numPages-1;loop_i>=0;loop_i--){


        (*arr)[loop_i]=PageFrameArray[loop_i]->pageHandle.pageNum;
    }



    return *arr;
    
    
    
}



// Get which pages are marked dirty
//

bool *getDirtyFlags (BM_BufferPool *const bm){ 

// Get the pointer to the PageFrame
    PageFrame *pf;
// Point it to the mgmtData struct
    pf=bm->mgmtData;


// Create a variable dirtyFlag: dirty or not dirty
    bool *dirtyFlag; // Holds the dirty status of the page

 // Allocate an array to hold the dirtyFlag Status of all pages
 dirtyFlag = (bool*)malloc(sizeof(bool)*bm->numPages);

printf("dirty flag of the page from inside function getDirtyFlags is: \n",dirtyFlag);
    int loop_i;
    
    for(loop_i = 0; loop_i<bm->numPages; loop_i=loop_i+1){

	printf("For page number, %d  , dirtyFlag is: %d",loop_i,dirtyFlag);
        dirtyFlag[loop_i] = PageFrameArray[loop_i]->dirty;
    }

    // Return dirtyFlag status: Pointer to the array allocated on the heap
    //
    //
    return dirtyFlag;

    
}





// This function gets the fix counts for a page
int *getFixCounts (BM_BufferPool *const bm){  

    // Variable to store Fixcount
    int *fixCount;

    printf("Working till 1 in function getFixCounts \n");

    // Get the pointer to the page Frame
    PageFrame *pf;

     printf("Working till 2 in function getFixCounts \n");


    // Get the auxilliary information stored in mgmtData
    pf=bm->mgmtData;

   printf("Working till 3 in function getFixCounts \n");

    
    
    // Get the fixCount
    
    fixCount = (int*)malloc(sizeof(int)*bm->numPages);
    
    printf("fixCount from inside function getFixCounts is:%d \n",fixCount);
    

    // loop variable
    int loop_i;


    // loop over all pages
    
    
    for(loop_i = 0; loop_i<(bm->numPages); loop_i=loop_i+1){

        printf("loop_i inside function getFixCounts is: %d \n", loop_i);
        fixCount[loop_i] = PageFrameArray[loop_i]->fixCount;
    }
    
    
    //return the fixCount for the page
    return fixCount; //Normal termination here

}


// THis function gets how many Read and Write operations have been done
//
//

int getNumReadIO (BM_BufferPool *const bm){


    printf("From inside function getNumReadIO: %d NumReadIO is: ", bufferRecord[currentBufferPoolNum].readNum);


    // Return readNum
    return bufferRecord[currentBufferPoolNum].readNum;
    }


// This function returns how many write opertations have been done
int getNumWriteIO (BM_BufferPool *const bm){

     printf("From inside function getNumWriteIO: %d NumReadIO is: ", bufferRecord[currentBufferPoolNum].writeNum);


    // Return writeNum;
    return bufferRecord[currentBufferPoolNum].writeNum;
}




