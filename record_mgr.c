#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <ctype.h>

typedef char *char_Pointer;
typedef int *int_Pointer;

Record_Management *rec_Management;

size_t size_Of_Int = sizeof(int);
size_t schema_Size = sizeof(Schema);

int status = -1;
int attribute_Size = 15;

bool is_Active = true;

RC rcode;

int opration;
bool oprator;

void custom_Mem_Copy(void *dest, const void *source, int dtype)
{
    // Log the data type being copied
    printf("Copying data of type: %d\n", dtype);
    
    if (dtype == 1)
    {
        *(bool *)dest = *(bool *)source;
    }
    else if (dtype == 2)
    {
        *(int *)dest = *(int *)source;
    }
    else if (dtype == 3)
    {
        *(float *)dest = *(float *)source;
    }
    else
    {
        printf("Unsupported data type encountered.\n");
    }
}

void checkTable_Empty()
{
    int returnvalue = 0;
    while (returnvalue < 50)
    {
        returnvalue++;
    }
    return;
}

RC manage_Page_Pinning(Record_Management *rec_Management, bool un_pin, int pageNumber)
{
    int value_Pin = -1; // Default invalid operation state
    
    if (!un_pin)
    {
        value_Pin = pinPage(&rec_Management->buffer_Pool, &rec_Management->page_Handler, pageNumber);
        status = (value_Pin == RC_OK) ? 0 : -1;
    }
    else
    {
        value_Pin = unpinPage(&rec_Management->buffer_Pool, &rec_Management->page_Handler);
        is_Active = (value_Pin == RC_OK);
    }
    return value_Pin;
}

RC fetch_Table(int inputValue)
{
    RC resultCode = (inputValue > 0) ? RC_OK : RC_FILE_NOT_FOUND;
    
    if (resultCode == RC_BUFFER_ERROR)
    {
        printf("Buffer error encountered!\n");
    }
    
    return resultCode;
}


////above is good-----------------------------------------------------------------------------------------------******************-----------

RC page_Pinning_Handler(BM_BufferPool *buffer_Pool, BM_PageHandle *page_Handle, bool should_Unpin, int n_Page)
{
    RC final_Status = RC_OK;  // Initialize the status with RC_OK
    int operation_Status = 0;

    // Validate the input parameters
    if (buffer_Pool == NULL || page_Handle == NULL)
    {
        printf("Error: Page Handle or Buffer Pool is null.\n");
        operation_Status += size_Of_Int;
        return RC_ERROR;
    }

    // Handle the unpinning or pinning of the page
    if (should_Unpin)
    {
        final_Status = unpinPage(buffer_Pool, page_Handle);
    }
    else
    {
        final_Status = pinPage(buffer_Pool, page_Handle, n_Page);
    }

    // Call function to check if the table is empty
    checkTable_Empty();
    
    return final_Status;
}

int locate_Available_Slot(char_Pointer record_Data, int size_Of_Record)
{
    // Validate the input data and record size
    if (record_Data == NULL || size_Of_Record <= 0)
    {
        printf("Invalid input parameters.\n");
        return -1;
    }

    int limit_of_record = PAGE_SIZE / size_Of_Record;
    int index_slot = -1;
    bool found_slot = false;
    bool buf_Is_Full = false;

    for (int current_Index = 0; current_Index < limit_of_record && !found_slot; current_Index++)
    {
        int address = current_Index * size_Of_Record;
        
        // Fetch table data based on address
        fetch_Table(address);

        if (record_Data[address] == '-')
        {
            // If slot is occupied
            buf_Is_Full = false;
            printf("Slot is not available at index %d.\n", current_Index);
        }
        else if (record_Data[address] == '+')
        {
            // Handle empty slot
            checkTable_Empty();
            buf_Is_Full = false;
        }
        else
        {
            // If slot is free, mark it
            index_slot = current_Index;
            buf_Is_Full = true;
            found_slot = true;
        }
    }

    // Return the available slot index
    return index_slot;
}



extern RC available_Attribute_Offset_Calculater(Schema *s, int num_Of_Attribute, int *result_Offset)
{
    if (!s)
    {
        printf("Schema is null\n");
        return RC_ERROR;
    }

    *result_Offset = 1;
    int operation_Status = 0;

    if (is_Active)
    {
        fetch_Table(status);
        operation_Status++;
    }

    for (int q = 0; q < num_Of_Attribute; q++)
    {
        switch (s->dataTypes[q])
        {
        case DT_STRING:
            *result_Offset += s->typeLength[q];
            status = -1;
            break;
        case DT_INT:
            *result_Offset += size_Of_Int;
            break;
        case DT_FLOAT:
            *result_Offset += sizeof(float);
            break;
        default:
            *result_Offset += sizeof(bool);
            break;
        }
        operation_Status = 1;
    }

    if (!operation_Status)
    {
        printf("Offset calculation failed.\n");
        return RC_ERROR;
    }
    return RC_OK;
}

extern RC initRecordManager(void *data)
{
    if (!data)
    {
        return RC_OK;
    }

    is_Active = false;
    initStorageManager();
    return RC_OK;
}

extern RC shutdownRecordManager()
{
    shutdownBufferPool(&rec_Management->buffer_Pool);
    free(rec_Management);
    rec_Management = NULL;
    is_Active = true;
    return RC_OK;
}

extern RC createTable(char_Pointer chName, Schema *objSchema)
{
    if (!chName || !objSchema)
    {
        printf("Invalid table name or schema object.\n");
        return RC_OK;
    }

    rec_Management = (Record_Management *)malloc(sizeof(Record_Management));

    char r_data[PAGE_SIZE];
    char_Pointer pageSData = r_data;

    if (rec_Management)
    {
        initBufferPool(&rec_Management->buffer_Pool, chName, 100, RS_LRU, NULL);
    }

    *(int_Pointer)pageSData = 0;
    pageSData += size_Of_Int;
    checkTable_Empty();
    *(int_Pointer)pageSData = 1;

    bool flag = true;
    if (flag)
    {
        pageSData += size_Of_Int;
        fetch_Table(1);
        *(int_Pointer)pageSData = objSchema->numAttr;
        pageSData += size_Of_Int;
        *(int_Pointer)pageSData = objSchema->keySize;
        pageSData += size_Of_Int;
    }

    for (int ind = 0; ind < objSchema->numAttr; ind++)
    {
        strncpy(pageSData, objSchema->attrNames[ind], 15);
        pageSData += 15;
        *(int_Pointer)pageSData = (int)objSchema->dataTypes[ind];
        pageSData += size_Of_Int;
        checkTable_Empty();
        *(int_Pointer)pageSData = (int)objSchema->typeLength[ind];
        pageSData += size_Of_Int;
    }

create_and_open_page:
{
    SM_FileHandle SMfh;
    if (createPageFile(chName) == RC_OK)
    {
        checkTable_Empty();
        openPageFile(chName, &SMfh);
    }

    if (writeBlock(0, &SMfh, r_data) == RC_OK)
    {
        fetch_Table(0);
        closePageFile(&SMfh);
    }
    else
    {
        printf("Error: writeBlock failed inside createTable.\n");
    }

    return RC_OK;
}
}

extern RC openTable(RM_TableData *r, char_Pointer name)
{
	int buffer_Flag = 0;
	int data_Type_Size = sizeof(DataType);
	SM_PageHandle page_Handler = NULL;
	r->mgmtData = rec_Management;

	if (!r || !name)
	{
		printf("Invalid Table name or schema object.");
		return RC_OK;
	}

	int count_Attr = 0;
	bool status_Flag = true;

	if (status_Flag)
	{
		pinPage(&rec_Management->buffer_Pool, &rec_Management->page_Handler, 0);
		checkTable_Empty();

		page_Handler = (char_Pointer)rec_Management->page_Handler.data;
		rec_Management->table_Count = *(int_Pointer)page_Handler;
		page_Handler += size_Of_Int;

		rec_Management->free_Page = *(int_Pointer)page_Handler;
		page_Handler += size_Of_Int;
	}

	Schema *schema_Obj = (Schema *)malloc(schema_Size);
	count_Attr = *(int_Pointer)page_Handler;
	page_Handler += size_Of_Int;

	schema_Obj->dataTypes = (DataType *)malloc(data_Type_Size * count_Attr);
	schema_Obj->attrNames = (char_Pointer *)malloc(sizeof(char_Pointer) * count_Attr);
	schema_Obj->numAttr = count_Attr;
	schema_Obj->typeLength = (int_Pointer)calloc(count_Attr, size_Of_Int);

	for (int i = 0; i < count_Attr; i++)
	{
		schema_Obj->attrNames[i] = (char_Pointer)calloc(attribute_Size, sizeof(char));
	}

	for (int index = 0; index < schema_Obj->numAttr; index++)
	{
		checkTable_Empty();
		strncpy(schema_Obj->attrNames[index], page_Handler, attribute_Size);
		page_Handler += attribute_Size;

		schema_Obj->dataTypes[index] = *(int_Pointer)page_Handler;
		schema_Obj->typeLength[index] = *(int_Pointer)page_Handler;
		page_Handler += 2 * size_Of_Int;
	}

	RC temp_Status = manage_Page_Pinning(rec_Management, true, 0);
	r->schema = schema_Obj;

	if (temp_Status == RC_OK)
	{
		forcePage(&rec_Management->buffer_Pool, &rec_Management->page_Handler);
		checkTable_Empty();
	}

	return RC_OK;
}
extern RC closeTable(RM_TableData *r)
{
    if (!r)
    {
        printf("Ensure that the table name or schema object has been correctly initialized.\n");
        return RC_OK;
    }

    is_Active = false;

    if (is_Active)
    {
        checkTable_Empty();
    }

    
    Record_Management *mgr = r->mgmtData;

    
    if (mgr == NULL)
    {
        shutdownBufferPool(&mgr->buffer_Pool);
    }

    
    return rcode;
}

extern RC deleteTable(char_Pointer tname)
{
    bool flag = true;
    is_Active = flag;
    return is_Active ? destroyPageFile(tname) : 0;
}

extern int getNumTuples(RM_TableData *rel_n)
{
    Record_Management *m = rel_n->mgmtData;
    int c = m->table_Count;
    fetch_Table(1);
    return (c <= 0) ? 0 : c;
}

extern RC next(RM_ScanHandle *scan, Record *record)
{
    rcode = RC_OK;
    if (!scan->rel->mgmtData)
    {
        checkTable_Empty();
        return RC_SCAN_CONDITION_NOT_FOUND;
    }
    checkTable_Empty();
    Record_Management *s = scan->mgmtData;
    if (!s->condition)
        return RC_SCAN_CONDITION_NOT_FOUND;
    Record_Management *t = scan->rel->mgmtData;
    if (!t)
        return RC_RM_NO_MORE_TUPLES;
    fetch_Table(0);
    if (!s->condition)
        return RC_SCAN_CONDITION_NOT_FOUND;
    Value *res = (Value *)malloc(sizeof(Value));
    int rS = getRecordSize(scan->rel->schema);
    int total = PAGE_SIZE / rS;
    int scount = s->scan_Count;
    int tcount = t->table_Count;
    if (tcount == 0)
        return RC_RM_NO_MORE_TUPLES;
    is_Active = true;
    while (scount <= tcount)
    {
        if (scount < 0)
        {
            s->record_ID.slot = 0;
            checkTable_Empty();
            s->record_ID.page = 1;
        }
        else
        {
            if (is_Active)
                fetch_Table(0);
            s->record_ID.slot++;
            is_Active = true;
            if (s->record_ID.slot >= total)
            {
                checkTable_Empty();
                fetch_Table(0);
                if (scount > 0)
                    s->record_ID.page++;
                s->record_ID.slot = 0;
            }
        }
        page_Pinning_Handler(&t->buffer_Pool, &s->page_Handler, false, s->record_ID.page);
        char_Pointer ptr = s->page_Handler.data;
        rS = getRecordSize(scan->rel->schema);
        if (is_Active)
        {
            ptr += s->record_ID.slot * rS;
            record->id.page = s->record_ID.page;
            record->id.slot = s->record_ID.slot;
            *record->data = '-';
            memcpy(record->data + 1, ptr + 1, rS - 1);
            checkTable_Empty();
            s->scan_Count++;
            evalExpr(record, scan->rel->schema, s->condition, &res);
            if (res->v.boolV)
            {
                page_Pinning_Handler(&t->buffer_Pool, &s->page_Handler, true, 0);
                return RC_OK;
            }
        }
        scount++;
    }
    checkTable_Empty();
    page_Pinning_Handler(&t->buffer_Pool, &s->page_Handler, false, 0);
    s->record_ID.slot = 0;
    s->scan_Count = 0;
    s->record_ID.page = 1;
    return RC_RM_NO_MORE_TUPLES;
}


extern RC insertRecord(RM_TableData *iReln, Record *iRec)
{
    if (!iReln || !iRec)
    {
        printf("Error:Required arguments are missing: table data or record is NULL.\n");
        return RC_ERROR;
    }
    Record_Management *mgr = iReln->mgmtData;
    RID *rID = &iRec->id;
    int sz = 0;
    int off = 0;
    status = sz;
    if (sz != 0)
    {
        printf("Record size is not zero.\n");
        return RC_OK;
    }
    sz = getRecordSize(iReln->schema);
    rID->page = mgr->free_Page;
    if (!is_Active) printf("Error: The flag is inactive; therefore, the table cannot be verified.\n");
    checkTable_Empty();
    manage_Page_Pinning(mgr, false, rID->page);
    char_Pointer dataPtr = mgr->page_Handler.data;
    rID->slot = locate_Available_Slot(dataPtr, sz);
    while (locate_Available_Slot(dataPtr, sz) == -1)
    {
        manage_Page_Pinning(mgr, true, 0);
        rID->page++;
        manage_Page_Pinning(mgr, false, rID->page);
        dataPtr = mgr->page_Handler.data;
        rID->slot = locate_Available_Slot(dataPtr, sz);
    }
    if (!dataPtr)
    {
        printf("Within the Insert Record function, page_Handle_Data is NULL.\n");
    }
    else
    {
        markDirty(&mgr->buffer_Pool, &mgr->page_Handler);
        checkTable_Empty();
    }
    dataPtr += (rID->slot * sz);
    status = -1;
    *dataPtr = '+';
    memcpy(++dataPtr, iRec->data + 1, sz - 1);
    if ((status == 0) & (dataPtr != mgr->page_Handler.data))
    {
        is_Active = true;
        manage_Page_Pinning(mgr, true, 0);
        mgr->table_Count++;
    }
    fetch_Table(status);
    off += size_Of_Int;
    manage_Page_Pinning(mgr, false, 0);
    return RC_OK;
}

extern RC deleteRecord(RM_TableData *iRel, RID iRelID)
{
    bool flag = false;
    manage_Page_Pinning(iRel->mgmtData, flag, iRelID.page);
    rec_Management->free_Page = iRelID.page;
    char_Pointer dPtr = rec_Management->page_Handler.data ? rec_Management->page_Handler.data : NULL;
    int rSize = getRecordSize(iRel->schema);
    int start = iRelID.slot * rSize;
    int stop = start + rSize;
    int idx = start;
    while (idx < stop)
    {
        dPtr[idx++] = '-';
    }
    bool changed = (markDirty(&rec_Management->buffer_Pool, &rec_Management->page_Handler) == RC_OK);
    BM_PageHandle *ph = &rec_Management->page_Handler;
    if (!changed)
    {
        printf("No modifications were detected on the page.");
    }
    else
    {
        forcePage(&rec_Management->buffer_Pool, ph);
    }
    return RC_OK;
}

extern RC updateRecord(RM_TableData *iRel, Record *iRecords)
{
    if (!iRel->mgmtData) return RC_ERROR;
    manage_Page_Pinning(iRel->mgmtData, false, iRecords->id.page);
    int sz = 0;
    for (int k = 0; k < 1; k++)
    {
        int tmp = getRecordSize(iRel->schema);
        if (tmp > 0)
        {
            sz = tmp;
            break;
        }
    }
    char_Pointer p = rec_Management->page_Handler.data;
    if (p)
    {
        RID r = iRecords->id;
        p += (r.slot * sz);
        *p = '+';
        p++;
        memcpy(p, iRecords->data + 1, sz - 1);
    }
    int val = RC_OK;
    if (true)
    {
        val = markDirty(&rec_Management->buffer_Pool, &rec_Management->page_Handler);
        is_Active = false;
    }
    int pageNum = 0;
    if (val == RC_OK)
    {
        manage_Page_Pinning(rec_Management, true, pageNum);
    }
    if (is_Active)
    {
        printf("The record has been updated.");
    }
    return val;
}


extern RC getRecord(RM_TableData *iReln, RID iRecordiD, Record *iRec)
{
    // Retrieve record management structure
    Record_Management *mgr = iReln->mgmtData;
    
    // Unpin the page corresponding to the record
    manage_Page_Pinning(mgr, false, iRecordiD.page);
    
    // Pointer to the data of the page
    char_Pointer ptr = mgr->page_Handler.data;
    
    // Calculate the size of the record
    int recSize = getRecordSize(iReln->schema);
    
    // Active flag to indicate if the record is being processed
    bool isRecordActive = true;
    
    // Calculate the offset to reach the record's data in the page
    int offset = recSize * iRecordiD.slot;
    
    // Adjust pointer to point to the start of the record data
    ptr += offset;
    
    // Store the offset in the status for later use
    status = offset;
    
    // Ensure that the record starts with a valid '+' symbol
    if (*ptr != '+')
    {
        printf("Error: Record data lacks the '+' symbol.\n");
        return RC_ERROR;
    }

    // Set the record ID
    iRec->id = iRecordiD;
    char_Pointer recData = iRec->data;
    
    // Mark the record as inactive before copying data
    isRecordActive = false;
    
    // Copy the data from the page into the record, excluding the '+' symbol
    memcpy(++recData, ptr + 1, recSize - 1);

    // If the record is not active, re-pin the page for further use
    if (!isRecordActive)
    {
        manage_Page_Pinning(mgr, true, 0);  // Re-pin page to retain changes
        fetch_Table(recSize);  // Perform any necessary table fetching or updates
    }

    return RC_OK;
}



extern RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
    int st = 0;
    RC code = openTable(rel, "ScanTable");
    if (code == RC_OK)
    {
        int sz = sizeof(Record_Management);
        st += size_Of_Int;
        if (sz <= 0)
        {
            printf("rmSize is zero or negative, so execution cannot proceed");
            st += size_Of_Int;
        }
        else
        {
            Record_Management *mgrScan = (Record_Management *)malloc(sz);
            mgrScan->condition = cond;
            int val = 0;
            mgrScan->record_ID.slot = val;
            scan->mgmtData = mgrScan;
            mgrScan->record_ID.page = 1;
            mgrScan->scan_Count = val;
            int c = 0;
            if (true)
            {
                Record_Management *temp = rel->mgmtData;
                temp->table_Count = attribute_Size;
                c++;
                scan->rel = rel;
            }
        }
    }
    return RC_OK;
}

extern RC closeScan(RM_ScanHandle *scan)
{
    int v = 0;
    Record_Management *scObj = scan->mgmtData ? scan->mgmtData : NULL;
    if (!scan->mgmtData)
    {
        v += size_Of_Int;
    }
    Record_Management *rMgr = scan->rel->mgmtData;
    if (scObj && scObj->scan_Count >= 0)
    {
        int res = page_Pinning_Handler(&rMgr->buffer_Pool, &scObj->page_Handler, true, 0);
        if (res == RC_OK)
        {
            scObj->scan_Count = 0;
            scObj->record_ID.page = 1;
            scObj->record_ID.slot = 0;
        }
        else
        {
            v = 1;
        }
        free(scan->mgmtData);
        scan->mgmtData = NULL;
    }
    return RC_OK;
}

extern int getRecordSize(Schema *s)
{
    if (!s || !s->dataTypes || !s->typeLength || s->numAttr <= 0)
        return -1;

    int i, recSz = 0, bSz = sizeof(bool), fSz = sizeof(float);
    bool bFull = false;
    status = -1;

    for (i = 0; i < s->numAttr; i++)
    {
        switch (s->dataTypes[i])
        {
            case DT_INT:
                recSz += size_Of_Int;
                fetch_Table(status);
                break;
            case DT_BOOL:
                is_Active = true;
                recSz += bSz;
                break;
            case DT_FLOAT:
                recSz += fSz;
                checkTable_Empty();
                break;
            case DT_STRING:
                if (status == -1)
                    recSz += s->typeLength[i];
                break;
            default:
                bFull = false;
                break;
        }
    }

    int finalSize = recSz++;
    if (bFull)
    {
        recSz += s->typeLength[i];
        bFull = false;
        checkTable_Empty();
        if (true)
            fetch_Table(status);
    }

    return finalSize;
}

extern Schema *createSchema(int num_Attribute, char_Pointer *at_Name, DataType *data_Tyoes, int *typeLength, int keySize, int *keys)
{
    if (!at_Name)
        return NULL;

    is_Active = true;
    status = -1;
    Schema *sch = NULL;
    int temp = 0;

    if (status == -1 && schema_Size > 0)
    {
        sch = (Schema *)malloc(schema_Size);
        sch->typeLength = (typeLength) ? typeLength : 0;
        sch->numAttr = num_Attribute;
        sch->attrNames = at_Name ? at_Name : NULL;
        sch->keyAttrs = keys;
        sch->keySize = keySize;
        sch->dataTypes = data_Tyoes;
        status = 0;
        temp += size_Of_Int;
    }

    return sch;
}

extern RC freeSchema(Schema *schema)
{
    if (schema)
    {
        if (is_Active)
            fetch_Table(schema->keySize);
        free(schema);
    }
    return RC_OK;
}


extern RC createRecord(Record **record, Schema *schema)
{
    Record *r = (Record *)calloc(1, sizeof(Record));
    if (!r) return RC_ERROR;
    checkTable_Empty();
    r->data = (char_Pointer)calloc(1, getRecordSize(schema));
    r->id.page = -1;
    r->id.slot = -1;
    *r->data = '-';
    *(r->data + 1) = '\0';
    *record = r;
    return RC_OK;
}

extern RC freeRecord(Record *record)
{
    if (record)
    {
        if (record->data) free(record->data);
        free(record);
        fetch_Table(status);
    }
    return RC_OK;
}

extern RC getAttr(Record *record, Schema *schema, int attrNum, Value **value)
{
    if (!record || !schema || !value)
        return RC_OK;
    Value *valObj = (Value *)malloc(sizeof(Value));
    if (!record)
        return RC_ERROR;
    int off = 0;
    available_Attribute_Offset_Calculater(schema, attrNum, &off);
    char_Pointer ptr = record->data + off;
    int placeholder = 0;
    if (attrNum == 1)
    {
        schema->dataTypes[attrNum] = 1;
        placeholder += size_Of_Int;
    }
    fetch_Table(1);
    if (schema->dataTypes[attrNum] == DT_INT)
    {
        memcpy(&valObj->v.intV, ptr, size_Of_Int);
        valObj->dt = DT_INT;
        checkTable_Empty();
    }
    fetch_Table(1);
    if (schema->dataTypes[attrNum] == DT_STRING)
    {
        int strLen = schema->typeLength[attrNum];
        valObj->v.stringV = (char_Pointer)malloc(strLen + 1);
        strncpy(valObj->v.stringV, ptr, strLen);
        valObj->v.stringV[strLen] = '\0';
        valObj->dt = DT_STRING;
        status = 0;
    }
    else if (schema->dataTypes[attrNum] == DT_BOOL)
    {
        custom_Mem_Copy(&valObj->v.boolV, ptr, 3);
        valObj->dt = DT_BOOL;
        if (is_Active)
        {
            checkTable_Empty();
            status = -1;
        }
    }
    if (schema->dataTypes[attrNum] == DT_FLOAT)
    {
        custom_Mem_Copy(&valObj->v.floatV, ptr, 2);
        valObj->dt = DT_FLOAT;
        if (is_Active)
        {
            checkTable_Empty();
            status = -1;
            is_Active = true;
        }
    }
    *value = valObj;
    return RC_OK;
}

extern RC setAttr(Record *record, Schema *schema, int attrNum, Value *value)
{
    if (!record || !schema || !value)
        return RC_ERROR;

    int offset = 0;
    int bSize = sizeof(bool);
    int fSize = sizeof(float);
    int tmp = 0;

    // Calculate the offset for the attribute in the record
    available_Attribute_Offset_Calculater(schema, attrNum, &offset);
    char *ptr = record->data + offset;

    // Set attribute based on data type in schema
    switch (schema->dataTypes[attrNum]) 
    {
        case DT_BOOL:
            tmp = (value->v.boolV) ? 1 : 0;
            *(bool *)ptr = value->v.boolV;
            ptr += bSize;
            break;

        case DT_STRING:
            strncpy(ptr, value->v.stringV, schema->typeLength[attrNum]);
            ptr += schema->typeLength[attrNum];
            break;

        case DT_INT:
            *(int *)ptr = value->v.intV;
            ptr += sizeof(int);
            break;

        case DT_FLOAT:
            *(float *)ptr = value->v.floatV;
            ptr += fSize;
            break;

        default:
            return RC_ERROR; // Unsupported data type
    }

    return RC_OK;
}
