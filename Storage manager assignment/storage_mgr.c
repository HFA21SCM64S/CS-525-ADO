
/*

#ifndef STORAGE_MGR_H
#define STORAGE_MGR_H

*/

#include "dberror.h"
#include "storage_mgr.h"
#include <sys/stat.h>
#include <unistd.h>

/* manipulating page files */



FILE* my_file_pointer;    // Create a pointer to a struct of type FILE which holds the stream to be connected to the file.

/********************************************************/
/*  This function initializes the file pointer to NULL
 *  That was we get rid of the dangling pointer and we can
 *  also use this NULL status to write conditional statements
 *  using the file pointer
   
 */

void initStorageManager (void){

my_file_pointer=NULL;   // Initialise this file pointer to NULL so that there are no dangling pointers

}

/*************************************************************/

/* This function creates a PageFile of size PAGE_SIZE
 * and initializes its contents to null '\0'
 */

RC createPageFile (char *fileName){

my_file_pointer= fopen(fileName, "w+");   // open a file with file name in string fileName

int file_descriptor=fileno(my_file_pointer); // Obtain the file descriptor for the file for  low level I/O

ftruncate(file_descriptor,PAGE_SIZE);  // increase the size of file to PAGE_SIZE

// int byte_count=0;  // Diagnostic code to check the size of file

char ch;  // Create a character variable to be used for initializing the contents of the file to '\0_'

ch=fgetc(my_file_pointer);  // Read character from the stream: Notice advances the stream position by 1_

while(ch!=EOF){  // While the character is not EOF

fseek(my_file_pointer,-1L,SEEK_CUR);  // Go back to the previous position

fputc('\0',my_file_pointer); // Insert null character at this position. Advances the stream position by 1_

// byte_count=byte_count+1;  // Diagnostic code to check the file size: Increase the byte count variable by 1_

ch=fgetc(my_file_pointer); // Read character at this new position. Advances the stream pointer by 1_

}

// printf("The number of bytes in the file is: %d \n",byte_count);  // Diagnostic code: Check that the count of bytes is PAGE_SIZE (4096)

rewind(my_file_pointer); // restore the file pointer back.

fclose(my_file_pointer); // close the file

return RC_OK; // return RC_OK
}

/******************************************************************************/

/* This function open the page file and initializes the file Handler.
 * The description of the fields of the file handler struct is as defined
 * in the assignment document;
 *  fileName: type (string) Holds the file name
 *  totalNumPages: type (int) Holds the total number of pages
 *  curPagePos: type (int) Holds the page number of the current page
 *  mgmtInfo  : type (void*) Holds the file pointer
 */

RC openPageFile (char *fileName, SM_FileHandle *fHandle){

my_file_pointer=fopen(fileName,"r+"); //open an existing file for reading. If the file does not exist, this returns a NULL pointer

if(my_file_pointer==NULL){  // If the file didnot exist. return RC_FILE_NOT_FOUND

return RC_FILE_NOT_FOUND;  // If the file pointer is NULL, it means that it is not connected to any page file - meaning file does not exist.

}

else{   // If the file pointer is not null, it means that the file pointer is connected to a file (and the file exists)

struct stat my_file_pointer_attributes;  // struct to hold the attributes of the file in the struct variable:my_file_pointer_attributes

stat(fileName,&my_file_pointer_attributes); // Use the stat function and fill in the struct variable: my_file_pointer_attributes

int file_size=my_file_pointer_attributes.st_size; // Get the size of the file using st_size member field of the struct variable

(*fHandle).fileName=fileName; // Initialize the fileName member field of the fileHandle

(*fHandle).totalNumPages=file_size/PAGE_SIZE;  // Initialize the totalNumPages field of the file handle

//printf ("The total number of pages in the file at point 1 is:%d \n",(*fHandle).totalNumPages);  // Diagnostic code to confirm the total number of pages are correctly calculated

(*fHandle).curPagePos=0; // Set the current pageNumber to 0_. First page is page number 0_

(*fHandle).mgmtInfo=my_file_pointer; // Store the file pointer in the mgmtInfo field of the fileHandle

return RC_OK;  // return RC_OK

}

}


/***********************************************************************/
/* This function closes the page file
 */

RC closePageFile (SM_FileHandle *fHandle){

if((fHandle->mgmtInfo)!=NULL){   // If the file pointer is not NULL (meaning that the file pointer is connected to file, then:

fclose((fHandle->mgmtInfo));   // close the file

return RC_OK;  // return RC_OK

}
else{

return RC_FILE_POINTER_NOT_ATTACHED_TO_FILE;



}

}

/************************************************************************/
/* This function removes the page file
 */

RC destroyPageFile (char *fileName){

int remove_status=remove(fileName);   // This function removes the file whose name is fileName.

if(remove_status==0){  // If remove_status is 0_, file is removed

return RC_OK;// If file is removed, return RC_OK

}
else{

return RC_FILE_NOT_FOUND;  // If remove_status is not 0_, File could not be found

}


}


/***********************************************************************/

/* reading blocks from disc */

/* This function reads blocks from a disk into a page in memory.
 * The page number to be read is specified in pageNum.
 * The memory location in which the page is to be read
 * is specified using memPage. The file is accesses using
 * the field of struct SM_FileHandle
 */  

RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){


if((fHandle->mgmtInfo) == NULL){  // Check that the File Handle is connected to an actual file


return RC_FILE_POINTER_NOT_ATTACHED_TO_FILE;  // If File handle is not connected to an actual file, terminate program

}


int no_of_pages=fHandle->totalNumPages;  // Total number of pages in the file
int max_page_no=no_of_pages-1;  // Maximum page number is one less because we are doing pageNUm from 0



if((pageNum<0) || (pageNum > max_page_no)){ // Check that the page number to write to exists

return RC_READ_NON_EXISTING_PAGE; // A non existing page is being read.

}

long offset_value=(pageNum*PAGE_SIZE);  // Offset measured from the start of the file. This is the start
                                        // of the page you wish to write to.

int seek_stat_1= fseek(fHandle->mgmtInfo,offset_value,SEEK_SET);//Position the file position indicator of the stream at
                                                        // the byte location where we wish to start writing
                                                        //
if(seek_stat_1 != 0){

return RC_FILE_POS_INDICATOR_FAILED_TO_POSITION;  // Unable to position the file position indicator properly

}

else{
int bytes_read=fread(memPage,sizeof(char),PAGE_SIZE,fHandle->mgmtInfo);// Write in binary mode starting from the correct
                                                                       // value of position indicator of the stream

if(bytes_read==PAGE_SIZE){
int seek_stat_2=fseek(fHandle->mgmtInfo,(-1)*PAGE_SIZE,SEEK_CUR); // Roll back file position indicator for the stream for PAGE_SIZE bytes
fHandle->curPagePos=offset_value/PAGE_SIZE;  // Update the curPagePos to the Block that was read
return RC_OK;

}
else{

return RC_READ_FAILED_CHECK_ERRNO;

}
}
}

/******************************************************************/
/* This function returns the current_page_position.
 * This is the page number that is stored in the 
 * curPagePos variable
 */
 
int getBlockPos (SM_FileHandle *fHandle){

if((fHandle->mgmtInfo) == NULL){    // Check that the File Handle is connected to an actual file


return RC_FILE_POINTER_NOT_ATTACHED_TO_FILE;   // If File handle is not connected to an actual file, terminate program

}

return(fHandle->curPagePos);  // This returns the current page position

}

/*******************************************************************/

/* This function reads the first block from the file into memory*/

RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){

int firstPageNumber=0;  // Get the page number corresponding to the first Block
readBlock(firstPageNumber,fHandle,memPage);  // readBlock returns RC_OK


}

/**********************************************************************/

/* This function reads the previous block from file to memory
 * The previous block is with reference to the curPagePos value
 * in the File Handle
 */

RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){

int current_page_number=fHandle->curPagePos;  // Get the current page position
int previous_page_number=current_page_number-1; // From the current page position, get the previous page position
readBlock(previous_page_number,fHandle,memPage);  // Read the previous page. readBlock returns RC_OK

}


/**********************************************************************/

/* This function reads the current block from file to memory
 * The current block is the curPagePos value stored
 * in the File Handle
 */


RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){

int current_page_number=fHandle->curPagePos;  // Get the current_page_position
readBlock(current_page_number,fHandle,memPage);  // Read the current page. readBlock returns RC_OK


}

/****************************************************************************/

/* This function reads the next block from file to memory
 * The next block is with reference to the curPagePos value
 * in the File Handle
 */
       
RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){

int current_page_number=fHandle->curPagePos; // Get the current_page_position
int next_page_number=current_page_number+1;  // From the current_page_position, get the next_page_position
readBlock(next_page_number,fHandle,memPage);  // This reads the next page. readBlock returns RC_OK

}

/*********************************************************************************/

/* This function reads the last block from the file into memory*/

RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
int last_page_number=(fHandle->totalNumPages)-1; // Get the last_page_number. pageNumber starts from zero. So the last pageNumber is: (totalNumPages-1)
readBlock(last_page_number,fHandle,memPage);  // readBlock returns RC_OK

}

/********************************************************************************/

/* writing blocks to a page file.
 * This function writes a page in memory
 * to a specific page number in the file
 */


RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){ // We want to write to page whose number is: pageNum


if((fHandle->mgmtInfo) == NULL){  // Check that the File Handle is connected to an actual file

return RC_FILE_POINTER_NOT_ATTACHED_TO_FILE;  // If File handle is not connected to an actual file, terminate program

}



int no_of_pages=fHandle->totalNumPages;  // Total number of pages in the file
int max_page_no=no_of_pages-1;  // Maximum page number is one less because we are doing pageNUm from 0_

/*

printf("no_of_pages is: %d \n",no_of_pages);   // Diagnostic code: Gives the number_of_pages
printf("max_page_no is: %d \n",max_page_no);   // Diagnostic code: Gives the maximum value of page number
printf("Comment me out: page number from inside writeBlock is:%d \n",pageNum); // Diagnostic code: Gives the value of input argument pageNum

*/
if((pageNum<0) || (pageNum > max_page_no)){ // Check that the page number to write to exists

return RC_WRITING_TO_NON_EXISTING_PAGE; // A non existing page is being written to.
	
}	
	
long offset_value=(pageNum*PAGE_SIZE);  // Offset measured from the start of the file. This is the start 
                                        // of the page you wish to write to. The file position
					// indicator of the stream is positioned to the start of the page you wish to
					// write to. offset_value is the starting point.	

int seek_stat_1= fseek(fHandle->mgmtInfo,offset_value,SEEK_SET);//Position the file position indicator of the stream at
                                                        // the byte location where we wish to start writing
							//
if(seek_stat_1 != 0){

return RC_FILE_POS_INDICATOR_FAILED_TO_POSITION;  // Unable to position the file position indicator properly

}
else{	 // File poistion indicator is positioned properly						


int bytes_written=fwrite(memPage,sizeof(char),PAGE_SIZE,fHandle->mgmtInfo);// Write in binary mode starting from the correct 
                                                                       // value of position indicator of the stream
                                     
if(bytes_written==PAGE_SIZE){  // Check that fwrite was able to write the full memory page.
// printf("ftell before rollback in writeBlock:%ld \n",ftell(fHandle->mgmtInfo)); Diagnostic code: tells the current value of the file position indicator for the stream
int seek_stat_2=fseek(fHandle->mgmtInfo,(-1)*PAGE_SIZE,SEEK_CUR); // Roll back file position indicator for the stream for PAGE_SIZE bytes so that it is at the start of current page
// printf("ftell after rollback in writeBlock:%ld \n",ftell(fHandle->mgmtInfo)); // Diagnostic code: tells the new value of the file position indicator for the stream (after Rollback)
fHandle->curPagePos=offset_value/PAGE_SIZE; // Update the current_page_position. The current position is the start of this page.

return RC_OK;  // If successful write, return RC_OK

}

else{

return RC_WRITE_FAILED;  // If for some reason write failed, return RC_WRITE_FAILED

}
}
}


/*******************************************************************************/

/* writing blocks to a page file.
 * This function writes a page in memory
 * to a current page number in the file
 */



RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){

int current_page_number=fHandle->curPagePos;  // Get the current page number
// printf("from inside writeCurrentBlock, current_page_number is:%d \n",current_page_number); //Diagnostic code to know what the value of current_page_number is
// printf("from inside writeCurrentBlock,totalNumPages is:%d \n",fHandle->totalNumPages); // Diagnostic code to know the total_number_of_pages in the file currently
writeBlock (current_page_number, fHandle,memPage);  // call to writeBlock() function specifying the current page number


}


/************************************************************************************/

/* This function appends an empty block to the file on the disk
 * and initializes all the bytes in the new block to '\0' (null value)
 */


RC appendEmptyBlock (SM_FileHandle *fHandle){

int currentSize=(fHandle->totalNumPages)*PAGE_SIZE;  // Current size is equal to (total number of pages)*(Page Size)
int newSize=currentSize+PAGE_SIZE; // This gives us the new file size with one more page than the previous
int check_file_increase_success=ftruncate(fileno(fHandle->mgmtInfo),newSize); // Increases the file size by one page

if(check_file_increase_success==0){ // Check whether the ftruncate was successful
(fHandle->totalNumPages) = (fHandle->totalNumPages)+1;   // Increase the totalNumPages field in the struct by 1_ because we added 1_ page
return RC_OK;

}

/*
else{

return RC_PAGE_ADDITION_FAILURE;


}
*/

}

/************************************************************************************/

/* This function increases the number of pages on the disk to specified number
 * given by numberOfPages. If the existing number of pages are already greater 
 * than numberOfPages, the function checks for that and retains the existing
 * number of pages.
 */

RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle){

int current_no_of_pages=fHandle->totalNumPages;  // The current number of pages in the file
int new_number_of_pages=numberOfPages;  // The new number of pages requested

if(current_no_of_pages<new_number_of_pages){  // If the current_no_of_pages<new_number_of_pages
int additional_pages_requested=(new_number_of_pages-current_no_of_pages); // How many additional pages are requested
int currentSize=(current_no_of_pages)*PAGE_SIZE;  // Current size in bytes
int newSize=currentSize+(additional_pages_requested)*(PAGE_SIZE); // New size in bytes
int check_file_increase_success=ftruncate(fileno(fHandle->mgmtInfo),newSize);  // increase the file_size to new size

if(check_file_increase_success==0){  // Check that the ftruncate call was successful
fHandle->totalNumPages=new_number_of_pages;  // Update the totalNumPages in the file handle
return RC_OK;   // return RC_OK  if ftruncate was successful

}
}
else{    // No action needed if ((current_number_of_pages) >= (new_number_of_pages))

return RC_OK;   // In that case return RC_OK any ways

}

}





/************************************************************
 *                    handle data structures                *
 ************************************************************/
/* 
 * typedef struct SM_FileHandle {
	char *fileName;
	int totalNumPages;
	int curPagePos;
	void *mgmtInfo;
} SM_FileHandle;

typedef char* SM_PageHandle;


*/


/************************************************************
 *                    interface                             *
 ************************************************************/
/* manipulating page files */

/*

extern void initStorageManager (void);
extern RC createPageFile (char *fileName);
extern RC openPageFile (char *fileName, SM_FileHandle *fHandle);
extern RC closePageFile (SM_FileHandle *fHandle);
extern RC destroyPageFile (char *fileName);

*/


/* reading blocks from disc */

/*
extern RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage);
extern int getBlockPos (SM_FileHandle *fHandle);
extern RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);

*/
/* writing blocks to a page file */

/*
extern RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC appendEmptyBlock (SM_FileHandle *fHandle);
extern RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle);

*/

/*
#endif
*/


