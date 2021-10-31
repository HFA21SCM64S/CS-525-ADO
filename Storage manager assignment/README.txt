**** This the README.txt file for the assignment 1 for CS525 ****

I have extensively commented the code and used highly descriptive variable names so that the code is self documenting.
A Makefile has also been provided. To compile the code, run the make utility in the source folder where the Makefile resides.
This will create an executable test_assign1 in the source folder. 
To run the executable, ./test_assign1.

/********************************************************************************************/

The basic idea of the application is to read information from the disk to the memory, process it and write the inform ation back to disk. The record keeping for which page is being read/written, how many
pages are in the file and which page is to be currently read is in the File Handle. The File Handle is able to access the file because mgmtInfo member of the file Handle strcut contains the file pointer

/**************************************************************************/

OUR CODE IS ABLE TO SATISFY ALL TESTS GIVEN IN test_assign1_1.c. IN addition, we have written three additional tests

void testAppendEmptyBlock(SM_FileHandle* fh);  // Patrick's additional testing function to test the function: RC appendEmptyBlock (SM_FileHandle *fHandle)
void testEnsureCapacity(SM_FileHandle* fh);    // Patrick's additional testing function to test ensure Capacity to test the function: RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle)
void testWriteAppendPageFunctions(void);       // Patrick's additional testing function that calls the above to function and also tests the function: RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)

In addition we have introduced three manifest constants for additional error conditions:

RC_WRITING_TO_NON_EXISTING_PAGE 5     error condition for writing to a non-existing page
RC_FILE_POINTER_NOT_ATTACHED_TO_FILE 6 error when the file pointer is NULL
RC_READ_FAILED_CHECK_ERRNO 7           error for READ failure for any system related issues (like faults etc).
RC_FILE_POS_INDICATOR_FAILED_TO_POSITION 8 error for the case where the file position indicator fails to be positioned.

/**************************************************************************************/




The following functions have been implemented in storage_mgr.c whose function prototypes are presented in storage_mgr.h



FILE* my_file_pointer: This is the file pointer to the file on the disk. This is used to access the file on the disk.

/*********************************************/
void initStorageManager (void):

This function initializes the the my_file_pointer to NULL. This is a global variable and is accessible to all functions in storage_mgr.c

/**********************************************/

RC createPageFile (char *fileName):
This function creates a new page file whose size is equal to PAGE_SIZE and initializes all bytes in the page file to '\0'.





/**********************************************/
RC openPageFile (char *fileName, SM_FileHandle *fHandle):

This function opens a previously created page file and associates a File Handle with it. The File Handle keeps track of useful information for the
file that is relevant to DBMS. The File Handle is a struct whose members are: 

fileName: String to hold the name of the file

totalNumPages: The total number of pages in the file. THis is a dynamic variable and is updated by the implemented functions as the number of pages change

curPagePos: This hold the current page number where the file position indicator of the stream is placed. This determines the byte from which the read/write of the page begins. With reads and writes,
the filepointer indicating the current position of the stream changes and this variable is dynamically updated by the functions using it.

mgmtInfo: This hold the file pointer and allows the other functions to work with the file by accessing this file pointer

This function initializes the values of these struct members as follows:

fileName: INitialised to the name of the file
totalNumPages: Initialised to 1 page (because initially the file is created with one page)
curPagePos: Initialised to 0 which is the first page
mgmtInfo: Initialised to the file pointer
/**********************************************/
RC closePageFile (SM_FileHandle *fHandle):

This function closes the pageFile






/**********************************************/

RC destroyPageFile (char *fileName):

This function deletes the page file with name fileName.





/**********************************************/

RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage):

This function reads blocks from a disk into a page in memory.
The page number to be read is specified in pageNum.
The memory location in which the page is to be read
is specified using memPage. The file is accesses using
the field of struct SM_FileHandle







/**********************************************/

int getBlockPos (SM_FileHandle *fHandle):

This function returns the current_page_position.
This is the page number that is stored in the
curPagePos variable







/**********************************************/


RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage):

This function reads the first block from the file into memory





/**********************************************/

RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage):

This function reads the previous block from file to memory
The previous block is with reference to the curPagePos value
in the File Handle







/**********************************************/

RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage):

This function reads the current block from file to memory
The current block is the curPagePos value stored
in the File Handle






/**********************************************/

RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage):

This function reads the next block from file to memory
The next block is with reference to the curPagePos value
in the File Handle

/**********************************************/

RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage):

This function reads the last block from the file into memory




/**********************************************/

RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage):

This function writes a page in memory (memPage)
to a specific page number (pageNum) in the file








/**********************************************/

RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage):

This function writes a page in memory
to a current page number in the file





/**********************************************/

RC appendEmptyBlock (SM_FileHandle *fHandle):

This function appends an empty block to the file on the disk
and initializes all the bytes in the new block to '\0' (null value)





/**********************************************/

RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle):

This function increases the number of pages on the disk to specified number
given by numberOfPages. If the existing number of pages are already greater
than numberOfPages, the function checks for that and retains the existing
number of pages.





/**********************************************/





/**********************************************/








/**********************************************/






/**********************************************/





/**********************************************/





