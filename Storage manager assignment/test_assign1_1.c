#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "storage_mgr.h"
#include "dberror.h"
#include "test_helper.h"
#include <assert.h>     // Assertion added by Patrick

// test name
char *testName;

/* test output files */
#define TESTPF "test_pagefile.bin"

/* prototypes for test functions */
static void testCreateOpenClose(void);
static void testSinglePageContent(void);
void testAppendEmptyBlock(SM_FileHandle* fh);  // Patrick's additional testing function
void testEnsureCapacity(SM_FileHandle* fh);    // Patrick's additional testing function
void testWriteAppendPageFunctions(void);       // Patrick's additional testing function
/* main function running all tests */
int
main (void)
{
  testName = "";
  
  initStorageManager();

    testCreateOpenClose();
    testSinglePageContent();
  //Additional test case: Check the writeBlock,writeCurrentBlock,appendEmpty and ensureCapacity function
    testWriteAppendPageFunctions();
  return 0;
}


/* check a return code. If it is not RC_OK then output a message, error description, and exit */
/* Try to create, open, and close a page file */
void
testCreateOpenClose(void)
{
  SM_FileHandle fh;

  testName = "test create open and close methods";

  TEST_CHECK(createPageFile (TESTPF));
  
  TEST_CHECK(openPageFile (TESTPF, &fh));
  ASSERT_TRUE(strcmp(fh.fileName, TESTPF) == 0, "filename correct");
  ASSERT_TRUE((fh.totalNumPages == 1), "expect 1 page in new file");
  ASSERT_TRUE((fh.curPagePos == 0), "freshly opened file's page position should be 0");

  TEST_CHECK(closePageFile (&fh));
  TEST_CHECK(destroyPageFile (TESTPF));

  // after destruction trying to open the file should cause an error
  ASSERT_TRUE((openPageFile(TESTPF, &fh) != RC_OK), "opening non-existing file should return an error.");

  TEST_DONE();
}

/* Try to create, open, and close a page file */
void
testSinglePageContent(void)
{
  SM_FileHandle fh;
  SM_PageHandle ph;
  int i;

  testName = "test single page content";

  ph = (SM_PageHandle) malloc(PAGE_SIZE);

  // create a new page file
  TEST_CHECK(createPageFile (TESTPF));
  TEST_CHECK(openPageFile (TESTPF, &fh));
  printf("created and opened file\n");
  
  // read first page into handle
  TEST_CHECK(readFirstBlock (&fh, ph));
  // the page should be empty (zero bytes)
  for (i=0; i < PAGE_SIZE; i++)
    ASSERT_TRUE((ph[i] == 0), "expected zero byte in first page of freshly initialized page");
  printf("first block was empty\n");
    
  // change ph to be a string and write that one to disk
  for (i=0; i < PAGE_SIZE; i++)
    ph[i] = (i % 10) + '0';
  TEST_CHECK(writeBlock (0, &fh, ph));
  printf("writing first block\n");

  // read back the page containing the string and check that it is correct
  TEST_CHECK(readFirstBlock (&fh, ph));
  for (i=0; i < PAGE_SIZE; i++)
    ASSERT_TRUE((ph[i] == (i % 10) + '0'), "character in page read from disk is the one we expected.");
  printf("reading first block\n");

  // destroy new page file
  TEST_CHECK(destroyPageFile (TESTPF));  
  
  TEST_DONE();
}




void testWriteAppendPageFunctions(void){

SM_FileHandle fh;
  SM_PageHandle ph;
  int i;

  testName = "test Write Append Page Functions: Patrick's Additional Test Function";

  ph = (SM_PageHandle) malloc(PAGE_SIZE);

  // create a new page file
  TEST_CHECK(createPageFile (TESTPF));
  TEST_CHECK(openPageFile (TESTPF, &fh));
  printf("created and opened file\n");

  // read first page into handle
  TEST_CHECK(readFirstBlock (&fh, ph));
  // the page should be empty (zero bytes)
  for (i=0; i < PAGE_SIZE; i++)
    ASSERT_TRUE((ph[i] == 0), "expected zero byte in first page of freshly initialized page");
  printf("first block was empty\n");

  // change ph to be a string and write that one to disk
  for (i=0; i < PAGE_SIZE; i++)
    ph[i] = (i % 10) + '0';
  TEST_CHECK(writeBlock (0, &fh, ph));
  printf("writing first block\n");

  // read back the page containing the string and check that it is correct
  TEST_CHECK(readFirstBlock (&fh, ph));
  for (i=0; i < PAGE_SIZE; i++)
    ASSERT_TRUE((ph[i] == (i % 10) + '0'), "character in page read from disk is the one we expected.");
  printf("reading first block\n");

  /***************************************************************************************************/

  // test for writeCurrentBlock and readCurrentBlock

  // change ph to be a string and write that one to disk : using the functionwriteCurrentBlock
  for (i=0; i < PAGE_SIZE; i++)
    ph[i] = (i % 10) + '0';
  TEST_CHECK(writeCurrentBlock (&fh, ph));
  printf("writing current block\n");

  // read back the page containing the string and check that it is correct : using the function readCurrentBlock
  TEST_CHECK(readCurrentBlock (&fh, ph));
  for (i=0; i < PAGE_SIZE; i++)
    ASSERT_TRUE((ph[i] == (i % 10) + '0'), "character in page read from disk is the one we expected.");
  printf("reading current block\n");
  



/****************************************************************************************************/
 // test for appendEmptyBlock
  
  testAppendEmptyBlock(&fh);  // Check that appendEmptyBlock is working
     
  /*
    int no_of_pages_at_start= (fh.totalNumPages);      // Currently the number of pages are

    appendEmptyBlock(&fh);  // append an empty page

    int no_of_pages_after_append_1=(fh.totalNumPages); // number of pages after appending 1_ page

    assert(no_of_pages_after_append_1==(no_of_pages_at_start+1));  // Check that the number of pages is one more than start

     appendEmptyBlock(&fh);  //Append another empty page

     int no_of_pages_after_append_2=(fh.totalNumPages);  // Number of pages after appending another page (so 2 have been appended compared to start

     assert(no_of_pages_after_append_2==(no_of_pages_at_start+2));
*/

 /**************************************************************************************************/
  // test for ensure Capacity
  
  testEnsureCapacity(&fh);  // Check that ensureCapcity is working
  
  /*
    int no_of_pages_at_start_test_capacity= (fh.totalNumPages);      // Currently the number of pages are

    ensureCapacity(6,&fh);  // Increase the capacity to 6_ pages

    int no_of_pages_after_ensure_capacity_1=(fh.totalNumPages); // number of pages after appending ensuring capacity to 6_ pages

    assert(no_of_pages_after_ensure_capacity_1==6);  // Check that the number of pages is 6_ now

     ensureCapacity(15,&fh);  //Increase capacity to 15_ pages

     int no_of_pages_after_ensure_capacity_2=(fh.totalNumPages);  // number of pages after appending ensuring capacity to 15_ pages

     assert(no_of_pages_after_ensure_capacity_2==(15)); // Check that the number of pages are 15_ now
*/
    


  // destroy new page file
  TEST_CHECK(destroyPageFile (TESTPF));

  TEST_DONE();


}


void testAppendEmptyBlock(SM_FileHandle* fh){


  int no_of_pages_at_start= (fh->totalNumPages);      // Currently the number of pages are

  appendEmptyBlock(fh);  // append an empty page

  int no_of_pages_after_append_1=(fh->totalNumPages); // number of pages after appending 1_ page

  assert(no_of_pages_after_append_1==(no_of_pages_at_start+1));  // Check that the number of pages is one more than start

  appendEmptyBlock(fh);  //Append another empty page

  int no_of_pages_after_append_2=(fh->totalNumPages);  // Number of pages after appending another page (so 2_ have been appended compared to start

  assert(no_of_pages_after_append_2==(no_of_pages_at_start+2));  // Check that the number of pages is 2_ more than the start


}


void testEnsureCapacity(SM_FileHandle* fh){

 int no_of_pages_at_start_test_capacity= (fh->totalNumPages);      // Currently the number of pages are

 ensureCapacity(6,fh);  // Increase the capacity to 6_ pages

 int no_of_pages_after_ensure_capacity_1=(fh->totalNumPages); // number of pages after appending ensuring capacity to 6_ pages

 assert(no_of_pages_after_ensure_capacity_1==6);  // Check that the number of pages is 6_ now

 ensureCapacity(15,fh);  //Increase capacity to 15_ pages

 int no_of_pages_after_ensure_capacity_2=(fh->totalNumPages);  // number of pages after appending ensuring capacity to 15_ pages

 assert(no_of_pages_after_ensure_capacity_2==(15)); // Check that the number of pages are 15_ now


}



