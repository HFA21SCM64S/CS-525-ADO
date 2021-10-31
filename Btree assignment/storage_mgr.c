#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <errno.h>
#include <string.h>
#include <limits.h>

#include "storage_mgr.h"  // Prototypes for storage_mgr.c
#include <unistd.h>





int creatFile;

// THis function initializes storage manager
void initStorageManager(void){   
	printf("Initializing Storage Manager \n"); 
	// return RC_OK;
}



// This function creates a page file
RC createPageFile (char *fileName){
	
	FILE *fp;   //get pointer to a file
	
	
	char * str;  // String to hold the file name   

	if(fileName==NULL){  //Check that a valid filename has been entered
		printf("pleas enter a valid fileName!!\n");
		return RC_FILE_NOT_FOUND; 
	}

	// Open the file for writing
	//
	fp = fopen(fileName,"w+");   
	str = (char *)malloc(PAGE_SIZE);   
	memset(str,'\0',PAGE_SIZE);	
	fwrite(str,PAGE_SIZE,1,fp); 
	
	free(str); 
	
	if(fp!=NULL)
	
		fclose(fp); // close it
	
	return RC_OK;

}


// This function opens a page file
RC openPageFile (char *fileName, SM_FileHandle *fHandle)
{

   // Pointer to file handle
    FILE *fh=NULL;
   
    // THis hold the file size
    long fileSize;

    // last check status
    long last_check = -1;
    
    // Open file for binary I/O
    fh=fopen(fileName, "rb+");
   
    if(fh!=NULL)
     {
             int result;

     	result=fseek(fh,0L,SEEK_END);
                if (result==0)
                {

                    last_check = ftell(fh);

      printf("variable last_check is: %d \n", last_check);

                    if(last_check != -1)  

		    {
			fileSize=last_check+1;                  
                        
                        fHandle ->totalNumPages =(int) (fileSize/(PAGE_SIZE));
 			fHandle ->fileName = fileName;      
                        if (fHandle-> curPagePos!=0)
                        {
                            fHandle ->curPagePos =0;
                        }
                    
                    fHandle ->mgmtInfo = fh;
                    return RC_OK;                               
			}
                    else{
                         return RC_GET_NUMBER_OF_BYTES_FAILED;
                         } 
                }
                else 
                    if(result==-1)
                {
                    printf("error,fseek failed");
                return RC_FILE_NOT_FOUND;
                }                  // This is to be carefully done. Check the status in gdb.
                            
                }
       



     
       
    else
    {
 return RC_FILE_NOT_FOUND;
    }
return RC_OK;
}
    


RC closePageFile(SM_FileHandle *fHandle){    

	if (fHandle == NULL){                         
		return RC_FILE_HANDLE_NOT_INIT;
	}

	//Initialize the values of fileHandle struct
	//Filename field
	fHandle->fileName = NULL;                
        //TotalNUmPages field
	fHandle->totalNumPages = 0;              
	//Current Page position
	fHandle->curPagePos = 0;                
	//mgmtInfo
	fHandle->mgmtInfo = NULL;                
       
	// Normal termination
	return RC_OK;

}



// THis function destroys the pageFile
RC destroyPageFile(char *fileName){  


	if (fileName == NULL){   //Check for NULL status of File

		return RC_FILE_NOT_FOUND;
	}


	int temp;
	
	temp = remove(fileName);   //temporarily store the result

	
	if (temp == -1){                //Result -1:Failure to destroy page file

	
		printf("Fail to destroy page file %s\n", fileName);
	
		return RC_FILE_NOT_FOUND;

	}

	
	return RC_OK;

}

// THis function reads a block/Page
//
//
RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    if(fHandle->mgmtInfo!=NULL) 

    {



       
       	    if((fHandle->totalNumPages)<=pageNum){
   
       		    return  RC_PAGE_NUMBER_OUT_OF_BOUNDRY;         
	    }
   	    int result;
   
   	    result=fseek(fHandle->mgmtInfo,PAGE_SIZE*pageNum*sizeof(char),SEEK_SET);
   
   	    if (result==0)
            {    
            
                                                        
           int readResult;

          // Check this with the debugger 
	   readResult= fread(memPage, sizeof(char),PAGE_SIZE,fHandle->mgmtInfo);
           
	   if(readResult==PAGE_SIZE)
            {

		    // Check this with debugger
                   fHandle->curPagePos=pageNum;               
            return RC_OK;
            }
            else if(readResult<PAGE_SIZE)
            {
                printf("fread error\n");
                return RC_READ_FAILED;
            }    
                
            }
            else
            {printf("fseek error\n");

             return RC_SET_POINTER_FAILED;
             }                                            
         

    }
    else
    {   
        return RC_FILE_NOT_FOUND;
        printf("mgminfo has no information\n");
    }
    
}

// This function gets the Block Position
int getBlockPos (SM_FileHandle *fHandle)
{
	
	if(fHandle != NULL)
		
		return fHandle->curPagePos; 
	
	else
	
	{
	
		printError(RC_FILE_HANDLE_NOT_INIT); 
	
		return RC_FILE_HANDLE_NOT_INIT; 
	
	}

}


// This function reads the first block
RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	if(fHandle != NULL)
	{
		int *fileDescriptor = (int *) fHandle->mgmtInfo; 
		
		long lseekResult = lseek(*fileDescriptor, 0, SEEK_SET); 
		if (lseekResult == -1) 
		{
			printf("file offset unchanged");
			return lseekResult;
		}
		
		long readResult = read(*fileDescriptor, memPage, PAGE_SIZE); 
		if (readResult == -1) // check whether read worked
		{
			printf("page not read into buffer"); 
			return readResult;
		}
			
		fHandle->curPagePos = 0; // assign pageNum to curPagePos
		return RC_OK;
	}
	else
	{
		printError(RC_FILE_HANDLE_NOT_INIT); // printout the message for the error
		return RC_FILE_HANDLE_NOT_INIT; // error: fHandle is null
	}
}


// THis function reads the previous block
RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{	
	if(fHandle != NULL)
	{
		if( fHandle->curPagePos == 0) // Check if current page is the first page
		{
			int *fileDescriptor = (int *) fHandle->mgmtInfo; // extract the fileDescriptor
		
			long lseekResult = lseek(*fileDescriptor, -PAGE_SIZE, SEEK_CUR); // Change file offset
			if (lseekResult == -1) // check whether lseek worked
			{
				printf("The file offset remains unchanged"); 
				return lseekResult;
			}
			
			long readResult = read(*fileDescriptor, memPage, PAGE_SIZE); 
			if (readResult == -1)
			{
				printf("page not read into buffer"); 
				return readResult;
			}
				
			fHandle->curPagePos -= 1; 
			return RC_OK;
		}
		else
		{
			printf("This is the first page\n");
			printError(RC_READ_NON_EXISTING_PAGE); 
			return RC_READ_NON_EXISTING_PAGE; 
		}
	}
	else
	{
		printError(RC_FILE_HANDLE_NOT_INIT); 
		return RC_FILE_HANDLE_NOT_INIT; 
	}
}



// THis function reads the current block into memory
RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) 

{
    if(fHandle==NULL)  
    {
        
        return RC_FILE_HANDLE_NOT_INIT;
    }

    if(fHandle->totalNumPages<1) 
    {
        printf("Total_page_number >=0\n");
        return RC_FILE_HANDLE_NOT_INIT; 
    }
    if(fHandle->curPagePos<0)   
    {
        printf("current_page_position not valid \n");
        return RC_READ_NON_EXISTING_PAGE;
    }

    if(fHandle->totalNumPages-1<fHandle->curPagePos) // current_page is invalid
    {
        printf("current_page_invalid\n"); 
        return RC_READ_NON_EXISTING_PAGE;
    }
    
    // File descriptor
    int *fileDescriptor; 
    fileDescriptor=(int *)fHandle->mgmtInfo; 
    
    // Result of read operation
    long readResult;
    int errno;
    errno=0;

    readResult=read(*fileDescriptor,memPage,PAGE_SIZE);  
    if(readResult=-1)   
    {  
        printf("reading block fails\n");
        printf("error type %d\n",errno);      
                 //output errno value
        printf("%s\n",strerror(errno)); 
       return RC_READ_NON_EXISTING_PAGE;
    }
    if(readResult<PAGE_SIZE)
    {
        printf("end_of_file\n",readResult );
    }
    return RC_OK;
    }

// This function reads the contents of next block into memory
RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)  
{
    if(fHandle==NULL)  
    {
        
        return RC_FILE_HANDLE_NOT_INIT;
    }

    if(fHandle->totalNumPages<1) 
    {
        
        return RC_FILE_HANDLE_NOT_INIT; 
    }
    if(fHandle->curPagePos<0)   // current_page invalid
    {
        
        return RC_READ_NON_EXISTING_PAGE;
    }

    if(fHandle->totalNumPages-1==fHandle->curPagePos) // current_page is last_page
    {
        
        return RC_READ_NON_EXISTING_PAGE;
    }
    
    fHandle->curPagePos+=1;   
   
    int *fileDescriptor;                        
    fileDescriptor=(int *)fHandle->mgmtInfo;    
     int errno;
	errno=0;
    long lseekResult;                           
    lseekResult=lseek(*fileDescriptor, PAGE_SIZE ,SEEK_CUR);
    if(lseekResult==-1)  
    {
	
        printf("page_not_read_into_buffer \n");                     
        printf("%s\n",strerror(errno));            
       
                return RC_READ_NON_EXISTING_PAGE;
    }

   
    long readResult;
    readResult=read(*fileDescriptor,memPage,PAGE_SIZE);  
    if(readResult=-1)  
    {  

        printf("reading block fails\n");
        printf("error type %d\n",errno);                      
        printf("%s\n",strerror(errno));           
       return RC_READ_NON_EXISTING_PAGE;
    }
    if(readResult<PAGE_SIZE)
    {
        
    }
    return RC_OK;
}


// THis function reads the contents of next block into memory
RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) 

{
    if(fHandle==NULL)  
    {
        printf("File_Handle_improperly_initialized\n");
        return RC_FILE_HANDLE_NOT_INIT;
    }

    if(fHandle->totalNumPages<1) //
    {
        printf("Total_page_number >=1 \n");
        return RC_FILE_HANDLE_NOT_INIT; 
    }
    if(fHandle->curPagePos<0)   
    {
        printf("current_page_position invalid\n");
        return RC_READ_NON_EXISTING_PAGE;
    }


    fHandle->curPagePos=fHandle->totalNumPages-1; 
    int *fileDescriptor;                        
    fileDescriptor=(int *)fHandle->mgmtInfo;    
    int totalOffset;                            //total_offset
    totalOffset=PAGE_SIZE*(fHandle->totalNumPages-1); 
     int errno;
	errno=0;

    long lseekResult;                           //Keep track of this
    lseekResult=lseek(*fileDescriptor, totalOffset ,SEEK_SET);
    if(lseekResult==-1)  
    {	
	
        printf("The page is not read into the buffer"); 
        printf("error type %d\n",errno);                       
        printf("%s\n",strerror(errno));            
                                   
                return RC_READ_NON_EXISTING_PAGE;
    }
   
    long readResult;
    readResult=read(*fileDescriptor,memPage,PAGE_SIZE);  
    if(readResult=-1)  
    {  
       
        printf("reading block fails\n");
       
       	printf("error type %d\n",errno);                       
       
       	printf("%s\n",strerror(errno));            
       
       return RC_READ_NON_EXISTING_PAGE;
    }
    if(readResult<PAGE_SIZE)
    {
        printf("readResult is:\n",readResult );
    }
    return RC_OK;

}




RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){

     while((fHandle->totalNumPages)<=pageNum) //check and append block
       {  appendEmptyBlock (fHandle);
       }
        if(fHandle->mgmtInfo==NULL)                 //if no such a pointer exists. return NOT FOUND.
        return RC_FILE_NOT_FOUND;
        int *fileDescriptor;
        int OFFSET=PAGE_SIZE*pageNum*sizeof(char);
  fileDescriptor=(int *)fHandle->mgmtInfo;
    if((fHandle->totalNumPages)>pageNum)
    {
            int seekresult;
            seekresult=fseek(fHandle->mgmtInfo,OFFSET,SEEK_SET);
            if(seekresult==0)
            {
                  if (fwrite(memPage, sizeof(char), PAGE_SIZE, fHandle->mgmtInfo)!=PAGE_SIZE)
                  return RC_WRITE_FAILED;                 
                                                        
                 fHandle->curPagePos=pageNum;                
                   return RC_OK;
            
                                                       
            }
            else
            {
                return RC_SET_POINTER_FAILED;
            }

            
                                                
    }
    else
    {   
        printf("appendblock failed\n");
        return RC_WRITE_FAILED;

    }
}



RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    if(fHandle!=NULL)
    {
        if(fHandle->curPagePos!=-1)
        {    
            int pageNum=fHandle->curPagePos;

            return writeBlock(pageNum, fHandle, memPage);
        }
    }
    
}



RC appendEmptyBlock (SM_FileHandle *fHandle){
    FILE *pf=fHandle->mgmtInfo;
    char* buff = (char *)calloc(PAGE_SIZE,sizeof(char));
    if(fHandle==NULL)  //FileHandle improperly initialized
    {
        printf("File Handle is not properly initialized\n");
        return RC_FILE_HANDLE_NOT_INIT;
    }

                                               
    int result;
    result=fwrite(buff, sizeof(char), PAGE_SIZE, pf);
    if(result!=0)
        {
            fHandle->totalNumPages+=1;

        free(buff);
        return RC_OK;                                       
        }
    else
    {
        return RC_WRITE_FAILED;
     }

    return RC_OK;
}



RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle)
{       
	int *fileDescriptor;
  fileDescriptor=(int *)fHandle->mgmtInfo;

	if(numberOfPages<=fHandle->totalNumPages)         
	{
		return RC_OK;
	}
	
	struct stat buffer;
	int fstat_return=fstat(*fileDescriptor, &buffer);
	if(fstat_return==-1)                 
	{
		
		return RC_WRITE_FAILED;
	}
	
	long totalbyte= numberOfPages*PAGE_SIZE-buffer.st_size;     
  
  SM_PageHandle newpage = (SM_PageHandle)malloc(totalbyte);
	
	int lseek_return=lseek(*fileDescriptor, 0, SEEK_END);
  if(lseek_return==-1)
	{
		
		return RC_WRITE_FAILED;
	}
  
  int write_return=write(*fileDescriptor, newpage, totalbyte);
  if(write_return==-1)   
	{
		
		return RC_WRITE_FAILED;
	}
	//update the totalNumPages
	fHandle->totalNumPages=numberOfPages;
	return RC_OK;
}

