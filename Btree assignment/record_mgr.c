

#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <string.h>
#include <stdlib.h>
#include <tgmath.h>

#define dummy_value_1 -1

/*  RMTableMgmtData stores metadata like number of tuples, first free page number, recorded size,
 *  buffer pool and page handle
 */
typedef struct RMTableMgmtData {

    int noOfTuples;
    int firstFreePageNumber;
    int recordSize;
    BM_PageHandle pageHandle;
    BM_BufferPool bufferPool;
} RMTableMgmtData;


SM_FileHandle fh_rec_mgr; //  Added by Patrick
int tombStonedRIDsList[10000]; // Added by Patrick

/* RMScanMgmtData stores scan details and condition */
typedef struct RMScanMgmtData {

    BM_PageHandle ph;
    RID rid; // current row that is being scanned
    int count; // no. of tuples scanned till now
    Expr *condition; // expression to be checked

} RMScanMgmtData;

RC initRecordManager(void *mgmtData) {
    return RC_OK;
}

RC shutdownRecordManager() {
    return RC_OK;
}

/*  This function creates a new table i.e a page file taking name of table and schema of table as parameters
 *  Meta data values are set and stored in page 1
 */
RC createTable(char *name, Schema *schema) {

    SM_FileHandle fh;
    RC rc;
    if ((rc = createPageFile(name)) != RC_OK) {
        return rc;
    }
    if ((rc = openPageFile(name, &fh)) != RC_OK) {
        return rc;
    }
    int i;
    char data[PAGE_SIZE];
    char *metaData = data;
    memset(metaData, 0, PAGE_SIZE);


    *(int *) metaData = 0; // Number of tuples
    metaData = metaData + sizeof(int); //increment char pointer

    *(int *) metaData = 2; // First free page is 1 because page 0 is reserved for metadata
    metaData = metaData + sizeof(int); //increment char pointer

    *(int *) metaData = getRecordSize(schema);
    metaData = metaData + sizeof(int); //increment char pointer

    *(int *) metaData = schema->numAttr; //set num of attributes
    metaData = metaData + sizeof(int);

    for (i = 0; i < schema->numAttr; i++) {

        strncpy(metaData, schema->attrNames[i], 20);    // set Attribute Name, assuming max field name is 20
        metaData = metaData + 20;

        *(int *) metaData = (int) schema->dataTypes[i];    // Set the Data Types of Attribute
        metaData = metaData + sizeof(int);

        *(int *) metaData = (int) schema->typeLength[i];    // Set the typeLength of Attribute
        metaData = metaData + sizeof(int);

    }
    for (i = 0; i < schema->keySize; i++) {
        *(int *) metaData = schema->keyAttrs[i]; // set keys for the schema
        metaData = metaData + sizeof(int);
    }

    if ((rc = writeBlock(1, &fh, data)) != RC_OK) { // Write all meta data info To 0th page of file
        return rc;
    }

    if ((rc = closePageFile(&fh)) != RC_OK) {
        return rc;
    }
    return RC_OK;
}

/*  This function is called to open existing table
 *  Once table is opened successfully, meta data stored in page 1 is copied to handler
 */
RC openTable(RM_TableData *rel, char *name) {

    SM_PageHandle metadata;
    RC rc;
    int i;
    Schema *schema = (Schema *) malloc(sizeof(Schema));


    RMTableMgmtData *tableMgmtData = (RMTableMgmtData *) malloc(sizeof(RMTableMgmtData));
    rel->mgmtData = tableMgmtData;
    rel->name = name;
    if ((rc = initBufferPool(&tableMgmtData->bufferPool, name, 10, RS_LRU, NULL)) != RC_OK) {
        return rc;
    }
    if ((rc = pinPage(&tableMgmtData->bufferPool, &tableMgmtData->pageHandle, 1)) !=
        RC_OK) { // pinning the 0th page which has meta data
        return rc;
    }
    metadata = (char *) tableMgmtData->pageHandle.data;
    tableMgmtData->noOfTuples = *(int *) metadata;
    metadata = metadata + sizeof(int);
    tableMgmtData->firstFreePageNumber = *(int *) metadata;
    metadata = metadata + sizeof(int);
    tableMgmtData->recordSize = *(int *) metadata;
    metadata = metadata + sizeof(int);
    schema->numAttr = *(int *) metadata;
    metadata = metadata + sizeof(int);

    schema->attrNames = (char **) malloc(sizeof(char *) * schema->numAttr);
    schema->dataTypes = (DataType *) malloc(sizeof(DataType) * schema->numAttr);
    schema->typeLength = (int *) malloc(sizeof(int) * schema->numAttr);
    for (i = 0; i < schema->numAttr; i++) {
        schema->attrNames[i] = (char *) malloc(20); //20 is max field length
    }

    for (i = 0; i < schema->numAttr; i++) {

        strncpy(schema->attrNames[i], metadata, 20);
        metadata = metadata + 20;

        schema->dataTypes[i] = *(int *) metadata;
        metadata = metadata + sizeof(int);

        schema->typeLength[i] = *(int *) metadata;
        metadata = metadata + sizeof(int);
    }
    schema->keySize = *(int *) metadata;
    metadata = metadata + sizeof(int);

    for (i = 0; i < schema->keySize; i++) {
        schema->keyAttrs[i] = *(int *) metadata;
        metadata = metadata + sizeof(int);
    }
    rel->schema = schema;

    if ((rc = unpinPage(&tableMgmtData->bufferPool, &tableMgmtData->pageHandle)) != RC_OK) {
        return rc;
    }
    return RC_OK;
}

/* This function closes opened table
 *  Before closing, total number of records in table is updated at page 1
 */
RC closeTable(RM_TableData *rel) {
    RC rc;
    RMTableMgmtData *rmTableMgmtData;
    SM_FileHandle fileHandle;
    rmTableMgmtData = rel->mgmtData;

    if ((rc = pinPage(&rmTableMgmtData->bufferPool, &rmTableMgmtData->pageHandle, 1)) != RC_OK) {
        return rc;
    }
    char *metaData = rmTableMgmtData->pageHandle.data;
    *(int *) metaData = rmTableMgmtData->noOfTuples;

    markDirty(&rmTableMgmtData->bufferPool, &rmTableMgmtData->pageHandle);

    if ((rc = unpinPage(&rmTableMgmtData->bufferPool, &rmTableMgmtData->pageHandle)) != RC_OK) {
        return rc;
    }
    if ((rc = shutdownBufferPool(&rmTableMgmtData->bufferPool)) != RC_OK) {
        return rc;
    }
    rel->mgmtData = NULL;
    return RC_OK;
}

/*  This function deletes table taking table name as input
 */
RC deleteTable(char *name) {
    RC rc;
    if ((rc = destroyPageFile(name)) != RC_OK) {
        return rc;
    }
    return RC_OK;
}

/* This function returns total number of records present in table
 * This value is fetched from page 1 of page file
 */
int getNumTuples(RM_TableData *rel) {

    RMTableMgmtData *rmTableMgmtData;
    rmTableMgmtData = rel->mgmtData;
    return rmTableMgmtData->noOfTuples;
}

// This function finds a page containing space and inserts specified record into it.



RC insertRecord(RM_TableData* rel, Record* record) {

//printf("The address of record passed as argument is: %p \n",(void*)record);
//printf("The address of TableData struct passed as argument is: %p \n", (void*)rel);

RMTableMgmtData* ptr_to_tbl_mgmt_struct=((*rel).mgmtData);

// printf("Values stored inside ptr_to_tbl_mgmt_struct is: %p \n", (void*)ptr_to_tbl_mgmt_struct);
//


int size_of_record=((*ptr_to_tbl_mgmt_struct).recordSize)+1;

// printf("size_of_record is: %d \n", size_of_record);
//
RID* ptr_to_record = &((*record).id);

// printf("Value stored inside ptr_to_record is: %p \n", (void*)ptr_to_record);

((*ptr_to_record).page)=((*ptr_to_tbl_mgmt_struct).firstFreePageNumber);

((*ptr_to_record).slot)=dummy_value_1;

char* address_of_data;  // Holds address of Data
char* address_of_slot;  // Holds address of slot

// Bring page into memory
pinPage(&((*ptr_to_tbl_mgmt_struct).bufferPool),
	       	&((*ptr_to_tbl_mgmt_struct).pageHandle), (*ptr_to_record).page); // Get the page in memory 
address_of_data = ((*ptr_to_tbl_mgmt_struct).pageHandle).data; // Get data pointer from the page


  int slot_count; // Variable to store count of available slots

  int total_no_of_slots = floor(PAGE_SIZE/size_of_record); // Total number of slots

  // printf("total_no_of_slots is: %d \n", total_no_of_slots);

    for (slot_count = 0; slot_count < total_no_of_slots; slot_count=slot_count+1) { // Go over each of the slots
     
	    if (*(address_of_data+(slot_count*size_of_record)) != '#') {
         
		    ((*ptr_to_record).slot) = slot_count;

	   //printf("Last slot is:%d \n ", ((*ptr_to_record).slot) );
	   //
            break;
        }
    }



 while ((*ptr_to_record).slot == dummy_value_1) {
        
        unpinPage(&((*ptr_to_tbl_mgmt_struct).bufferPool), &((*ptr_to_tbl_mgmt_struct).pageHandle));
        ((*ptr_to_record).page)=((*ptr_to_record).page)+1;    // go to next page

        pinPage(&((*ptr_to_tbl_mgmt_struct).bufferPool),
		       	&((*ptr_to_tbl_mgmt_struct).pageHandle), ((*ptr_to_record).page)); // Bring next oage into memory
        
	for (slot_count = 0; slot_count < total_no_of_slots; slot_count=slot_count+1) {
            
		if (*(address_of_data+(slot_count*size_of_record)) != '#') {  // Check that it is not '#'

                ((*ptr_to_tbl_mgmt_struct).firstFreePageNumber) = ((*ptr_to_record).page);
                 
		// printf("page number at point A is: %d \n", ((*ptr_to_tbl_mgmt_struct).firstFreePageNumber));
                
		((*ptr_to_record).slot) = slot_count;
		
		//printf("Last slot is:%d \n ", ((*ptr_to_record).slot) );

                break;
            }
        }
    }




    address_of_slot = address_of_data;

    // printf("address_of_slot at point 1 is: %p \n",(void*)address_of_slot);
    //
    markDirty(&((*ptr_to_tbl_mgmt_struct).bufferPool), &((*ptr_to_tbl_mgmt_struct).pageHandle)); // Because the page has been modified, 
                                                                                                 // mark it dirty - uaing BP function

    address_of_slot = address_of_slot + ((*ptr_to_record).slot)*size_of_record;

    // printf("address_of_slot at point 2 is: %p \n",(void*)address_of_slot);
    //
    *address_of_slot  = '#';
    address_of_slot = address_of_slot + 1;

    // printf("address_of_slot at point 3 is: %p \n",(void*)address_of_slot);
    //
    memcpy(address_of_slot,(*record).data,size_of_record-1); // Copy back

    // Remove page from memory
    unpinPage(&((*ptr_to_tbl_mgmt_struct).bufferPool), &((*ptr_to_tbl_mgmt_struct).pageHandle));
    // Now there is one more record. So increment no_of_tuples
    ((*ptr_to_tbl_mgmt_struct).noOfTuples)=((*ptr_to_tbl_mgmt_struct).noOfTuples)+1;
    // Restore id attribute of record to the correct value
    //
    (*record).id = *ptr_to_record;
    return RC_OK;





}






//  This method deletes specified record

RC deleteRecord(RM_TableData *rel, RID id) { 

    //printf("Address of input TableData is: %p \n",(void*)rel);
    //
    RMTableMgmtData* ptr_to_tbl_mgmt_struct=((*rel).mgmtData);

    // printf("Address stored in ptr_to_tbl_mgmt_struct is: %p \n",(void*)ptr_to_tbl_mgmt_struct);
    // Load the appropriate page into memory
    //
    pinPage(&((*ptr_to_tbl_mgmt_struct).bufferPool), 
		    &((*ptr_to_tbl_mgmt_struct).pageHandle), id.page); // LOad page into memory using pinPage method

    ((*ptr_to_tbl_mgmt_struct).noOfTuples)=((*ptr_to_tbl_mgmt_struct).noOfTuples)-1; //number of tuples is now one less after removing a record
    
    char* address_of_slot; // Address of slot
    char* address_of_data; // Address of data

    address_of_slot=0;
    address_of_data=0;

    int size_of_record = ((*ptr_to_tbl_mgmt_struct).recordSize) + 1; // size of record

    // printf("size_of_record is: %d \n", size_of_record);

    address_of_data = ((*ptr_to_tbl_mgmt_struct).pageHandle).data;
    //printf("address_of_data is:%p \n",(void*)address_of_data);
    address_of_slot = address_of_data;
    //printf("address_of_slot at point 1 is:%p \n",(void*)address_of_slot);

    address_of_slot = address_of_slot + (id.slot*size_of_record);
    //printf("address_of_slot at point 2 is:%p \n",(void*)address_of_slot);

    *address_of_slot = '$';
    // Because the page has been modified, mark it dirty
    markDirty(&((*ptr_to_tbl_mgmt_struct).bufferPool),
		    &((*ptr_to_tbl_mgmt_struct).pageHandle));
    // Unpin page
    unpinPage(&((*ptr_to_tbl_mgmt_struct).bufferPool),
		    &((*ptr_to_tbl_mgmt_struct).pageHandle));
    // At this point after marking the page dirty and unpinning, return
    return RC_OK;
} 






// This function updates specified record


RC updateRecord(RM_TableData* rel, Record* record) { 

    // printf("input address of TableData is: %p \n", (void*)rel);

    RMTableMgmtData* ptr_to_tbl_mgmt_struct=((*rel).mgmtData);

    // printf("Address in ptr_to_tbl_mgmt_struct is: %p \n", (void*)ptr_to_tbl_mgmt_struct);

    // Load page into memory
    pinPage(&((*ptr_to_tbl_mgmt_struct).bufferPool), &((*ptr_to_tbl_mgmt_struct).pageHandle), ((*record).id).page);
    
    char* address_of_data=0;
    
    address_of_data = ((*ptr_to_tbl_mgmt_struct).pageHandle).data;

    // printf("address_of_data at point 1 is: %p \n", (void*)address_of_data);

    RID record_id = (*record).id;

    RC status_1;

    int size_of_record = ((*ptr_to_tbl_mgmt_struct).recordSize) + 1;

    // printf("size_of_record is: %d \n", size_of_record);
    //
    address_of_data = (address_of_data + record_id.slot*size_of_record + 1);

    // printf("address_of_data at point 2 is: %p \n", (void*)address_of_data);

    memcpy(address_of_data, (*record).data, size_of_record-1);

    if ((status_1 = markDirty(&((*ptr_to_tbl_mgmt_struct).bufferPool),
				    &((*ptr_to_tbl_mgmt_struct).pageHandle))) != RC_OK) {

	// printf("status_1 at point 1 is:%d \n", status_1);    
        
	    return status_1;
    }

    if ((status_1 = unpinPage(&((*ptr_to_tbl_mgmt_struct).bufferPool),
				    &((*ptr_to_tbl_mgmt_struct).pageHandle))) != RC_OK) {
        // printf("status_1 at point 2 is:%d \n", status_1);

	   return status_1;
    }
    return RC_OK;
}





// This method gets specified record
 

RC getRecord(RM_TableData *rel, RID id, Record *record) { 

    
    // printf("input address of TableData is: %p \n", (void*)rel);
    //
    RMTableMgmtData* ptr_to_tbl_mgmt_struct=((*rel).mgmtData);

    // printf("address in ptr_to_tbl_mgmt_struct is: %p \n", (void*)ptr_to_tbl_mgmt_struct);

    RC status_1;

    if ((status_1 = pinPage(&((*ptr_to_tbl_mgmt_struct).bufferPool), 
				    &((*ptr_to_tbl_mgmt_struct).pageHandle), id.page)) != RC_OK) {

	//printf("status_1 at point 1 is:%d \n", status_1);
	//
        return status_1;
    }
    int size_of_record = ((*ptr_to_tbl_mgmt_struct).recordSize) + 1;

    printf("size_of_record is: %d \n", size_of_record);

    char* address_of_slot;

    address_of_slot=0;

    address_of_slot = ((*ptr_to_tbl_mgmt_struct).pageHandle).data;
    // printf("address in address_of_slot  at point 1 is: %p \n", (void*)address_of_slot);

    address_of_slot = address_of_slot + (id.slot*size_of_record);
    // printf("address in address_of_slot  at point 2 is: %p \n", (void*)address_of_slot);

    if (*address_of_slot != '#')
        return RC_TUPLE_WIT_RID_ON_EXISTING;
    else {
        char* request_data = record->data;
       
	// printf("address in request_data is: %p \n", (void*)request_data);

        memcpy(request_data, address_of_slot + 1,size_of_record-1);
        record->id = id;

    }
    if ((status_1 = unpinPage(&((*ptr_to_tbl_mgmt_struct).bufferPool),
				    &((*ptr_to_tbl_mgmt_struct).pageHandle))) != RC_OK) {
        return status_1;
    }
    return RC_OK;
}



/**
 * This method initialises the scan
                1) It initialises RMScanMgmtData structure
                2) If it is initialised, it returns RC_OK
 * @param rel
 * @param scan
 * @param cond
 * @return
 */
RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond) {

    scan->rel = rel;

    RMScanMgmtData *rmScanMgmtData = (RMScanMgmtData *) malloc(sizeof(RMScanMgmtData));
    rmScanMgmtData->rid.page = 2;
    rmScanMgmtData->rid.slot = 0;
    rmScanMgmtData->count = 0;
    rmScanMgmtData->condition = cond;

    scan->mgmtData = rmScanMgmtData;

    return RC_OK;
}

/**
 * This method gives the tuple that satisfies the scan
                1) If there are no tuples in the table it returns RC_RM_NO_MORE_TUPLES
                2) Else it starts scanning from the page number stored in RM_ScannedData
                3) It obtains a record and checks for the scan condition.
                4) If above is satisfies it will return RC_OK and updates record->data with the record details obtained from page file.
                5) Else it will take the next record and checks for condition, it repeats this till it finds a record which satisfies the condition or the end of table is reached
                6) If no record is found satisfying the condition it returns RC_RM_NO_MORE_TUPLES
 * @param scan
 * @param record
 * @return
 */
RC next(RM_ScanHandle *scan, Record *record) {

    RMScanMgmtData *scanMgmtData = (RMScanMgmtData *) scan->mgmtData;
    RMTableMgmtData *tmt;
    tmt = (RMTableMgmtData *) scan->rel->mgmtData;

    Value *result = (Value *) malloc(sizeof(Value));
    static char *data;

    int recordSize = tmt->recordSize + 1;
    int totalSlots = floor(PAGE_SIZE / recordSize);

    if (tmt->noOfTuples == 0)
        return RC_RM_NO_MORE_TUPLES;


    while (scanMgmtData->count <= tmt->noOfTuples) {
        if (scanMgmtData->count <= 0) {
            scanMgmtData->rid.page = 2;
            scanMgmtData->rid.slot = 0;

            pinPage(&tmt->bufferPool, &scanMgmtData->ph, scanMgmtData->rid.page);
            data = scanMgmtData->ph.data;

        } else {
            scanMgmtData->rid.slot++;
            if (scanMgmtData->rid.slot >= totalSlots) {
                scanMgmtData->rid.slot = 0;
                scanMgmtData->rid.page++;
            }

            pinPage(&tmt->bufferPool, &scanMgmtData->ph, scanMgmtData->rid.page);
            data = scanMgmtData->ph.data;
        }

        data = data + (scanMgmtData->rid.slot * recordSize) + 1;

        record->id.page = scanMgmtData->rid.page;
        record->id.slot = scanMgmtData->rid.slot;
        scanMgmtData->count++;

        memcpy(record->data, data, recordSize - 1);

        if (scanMgmtData->condition != NULL) {
            evalExpr(record, (scan->rel)->schema, scanMgmtData->condition, &result);
        } else {
            result->v.boolV == TRUE; // when no condition return all records
        }

        if (result->v.boolV == TRUE) {  //result was found
            unpinPage(&tmt->bufferPool, &scanMgmtData->ph);
            return RC_OK;
        } else {
            unpinPage(&tmt->bufferPool, &scanMgmtData->ph);
        }
    }

    scanMgmtData->rid.page = 2; //Resetting after scan is complete
    scanMgmtData->rid.slot = 0;
    scanMgmtData->count = 0;
    return RC_RM_NO_MORE_TUPLES;
}

/**
 * This method closes the scan
                1) It unpins the page
                2) Frees memory allocated with RMScanMgmtData
                3) If it closes properly, it returns RC_OK
 * @param scan
 * @return
 */
RC closeScan(RM_ScanHandle *scan) {
    RMScanMgmtData *rmScanMgmtData = (RMScanMgmtData *) scan->mgmtData;
    RMTableMgmtData *rmTableMgmtData = (RMTableMgmtData *) scan->rel->mgmtData;

    if (rmScanMgmtData->count > 0) {
        unpinPage(&rmTableMgmtData->bufferPool, &rmTableMgmtData->pageHandle); // unpin the page
    }

    // Free mgmtData
    free(scan->mgmtData);
    scan->mgmtData = NULL;
    return RC_OK;
}

/**
 * This method returns the record size
                1) It identifies the attributes from the passed schema
                2) It finds the corresponding datatype
                3) It then sums up the size that is occupied in bytes
                4) It then returns size occupied by all the attributes
 * @param schema
 * @return
 */
int getRecordSize(Schema *schema) {

    int size = 0, i;
    for (i = 0; i < schema->numAttr; ++i) {
        switch (schema->dataTypes[i]) {
            case DT_STRING:
                size = size + schema->typeLength[i];
                break;
            case DT_INT:
                size = size + sizeof(int);
                break;
            case DT_FLOAT:
                size = size + sizeof(float);
                break;
            case DT_BOOL:
                size = size + sizeof(bool);
                break;
            default:
                break;
        }
    }
    return size;
}

/**
 * This method creates a schema
                1) It allocates the memory to schema
                2) It then initialises the schema attributes
                3) It returns schema
 * @param numAttr
 * @param attrNames
 * @param dataTypes
 * @param typeLength
 * @param keySize
 * @param keys
 * @return
 */
Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys) {

    Schema *schema = malloc(sizeof(Schema));
    schema->numAttr = numAttr;
    schema->attrNames = attrNames;
    schema->dataTypes = dataTypes;
    schema->typeLength = typeLength;
    schema->keySize = keySize;
    schema->keyAttrs = keys;
    return schema;
}

/**
 * This method frees the schema
                    1) It frees the memory associated with the schema
                    2) If the schema is freed, it returns RC_OK
 * @param schema
 * @return
 */
RC freeSchema(Schema *schema) {

    free(schema);
    return RC_OK;
}

/**
 * This method creates a record
                1) It first obtains the size through the passed schema
                2) It then allocates memory
                3) It sets page and slot id to -1
                4) Once record is created, it returns RC_OK
 * @param record
 * @param schema
 * @return
 */
RC createRecord(Record **record, Schema *schema) {

    int recordSize = getRecordSize(schema);

    Record *newRecord = (Record *) malloc(sizeof(Record));
    newRecord->data = (char *) malloc(recordSize); // Allocate memory for data of record

    newRecord->id.page = -1; // page number is not fixed for empty record which is in memory
    newRecord->id.slot = -1; // slot number is not fixed for empty record which is in memory
    *record = newRecord; // set tempRecord to Record
    return RC_OK;
}

/**
 * This method frees the record
                1) It frees the record
                2) If the record is freed, it returns RC_OK
 * @param record
 * @return
 */
RC freeRecord(Record *record) {

    free(record);
    return RC_OK;
}

/**
 * This method determines the offset of the attribute in record
                1) This method determines the the offset of the attribute in record
                2) Once determined, it returns RC_OK
 * @param schema
 * @param attrNum
 * @param result
 * @return
 */
RC determineAttributOffsetInRecord(Schema *schema, int attrNum, int *result) {
    int offset = 0;
    int attrPos = 0;

    for (attrPos = 0; attrPos < attrNum; attrPos++) {
        switch (schema->dataTypes[attrPos]) {
            case DT_STRING:
                offset = offset + schema->typeLength[attrPos];
                break;
            case DT_INT:
                offset = offset + sizeof(int);
                break;
            case DT_FLOAT:
                offset = offset + sizeof(float);
                break;
            case DT_BOOL:
                offset = offset + sizeof(bool);
                break;
        }
    }
    *result = offset;
    return RC_OK;
}

/**
 * This method gets an attribute of the record
                1) It determines the attribute offset in the record using "determineAttributOffsetInRecord" method
                2) It copies the value from this offset based on the attributes type information into the value object passed
                3) Returns RC_OK on success
 * @param record
 * @param schema
 * @param attrNum
 * @param value
 * @return
 */
RC getAttr(Record *record, Schema *schema, int attrNum, Value **value) {

    int offset = 0;
    determineAttributOffsetInRecord(schema, attrNum, &offset);
    Value *tempValue = (Value *) malloc(sizeof(Value));
    char *string = record->data;
    string += offset;
    switch (schema->dataTypes[attrNum]) {
        case DT_INT:
            memcpy(&(tempValue->v.intV), string, sizeof(int));
            tempValue->dt = DT_INT;
            break;
        case DT_STRING:
            tempValue->dt = DT_STRING;
            int len = schema->typeLength[attrNum];
            tempValue->v.stringV = (char *) malloc(len + 1);
            strncpy(tempValue->v.stringV, string, len);
            tempValue->v.stringV[len] = '\0';
            break;
        case DT_FLOAT:
            tempValue->dt = DT_FLOAT;
            memcpy(&(tempValue->v.floatV), string, sizeof(float));
            break;
        case DT_BOOL:
            tempValue->dt = DT_BOOL;
            memcpy(&(tempValue->v.boolV), string, sizeof(bool));
            break;
    }
    *value = tempValue;
    return RC_OK;
}

/**
 * It sets the attributes of the record to the value passed for the attribute determined by "attrNum"
                1) It uses "determineAttributOffsetInRecord" for determining the offset at which the attibute is stored
                2) It uses the attribute type to write the new value at the offset determined above
                3) Once the attribute is set, RC_OK is returned
 * @param record
 * @param schema
 * @param attrNum
 * @param value
 * @return
 */
RC setAttr(Record *record, Schema *schema, int attrNum, Value *value) {
    int offset = 0;
    determineAttributOffsetInRecord(schema, attrNum, &offset);
    char *data = record->data;
    data = data + offset;

    switch (schema->dataTypes[attrNum]) {
        case DT_INT:
            *(int *) data = value->v.intV;
            break;
        case DT_STRING: {
            char *buf;
            int len = schema->typeLength[attrNum];
            buf = (char *) malloc(len + 1);
            strncpy(buf, value->v.stringV, len);
            buf[len] = '\0';
            strncpy(data, buf, len);
            free(buf);
        }
            break;
        case DT_FLOAT:
            *(float *) data = value->v.floatV;
            break;
        case DT_BOOL:
            *(bool *) data = value->v.boolV;
            break;
    }

    return RC_OK;
}


