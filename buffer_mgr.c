//my team

#include <stdio.h>
#include <stdlib.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <math.h>
#include <limits.h>
#include <string.h>
// Define missing return codes if they are not defined elsewhere
#ifndef RC_INVALID_PARAMETER
#define RC_INVALID_PARAMETER -1
#endif

#ifndef RC_MEMORY_ALLOCATION_ERROR
#define RC_MEMORY_ALLOCATION_ERROR -2
#endif
typedef struct Page {
    SM_PageHandle pageData;
    PageNumber pageID;
    int isDirty;
    int pinCount;
    int accessCount;
    int referenceCount;
} PageFrame;

int bufferCapacity = 0;
int tailIndex = 0;
int writeOperations = 0;
int cachehits = 0;
int clockHand = 0;
int lfuIndex = 0;

// Function implementations

extern void FIFO(BM_BufferPool *const bp, PageFrame *tgtPg) {
    if (!bp || !bp->mgmtData) return;  // Added a null check for safety
    PageFrame *frames = bp->mgmtData;  // Removed explicit cast
    int trialCount = 0;
    int currentIndex = tailIndex % bufferCapacity; // Assuming tailIndex and bufferCapacity are declared elsewhere
    
    while (trialCount < bufferCapacity) {
        PageFrame *currentFrame = &frames[currentIndex];
        
        if (currentFrame->pinCount == 0) { // Eligible for replacement
            // Flush if dirty
            if (currentFrame->isDirty) {
                SM_FileHandle fh;
                if (openPageFile(bp->pageFile, &fh) == RC_OK) {
                    if (writeBlock(currentFrame->pageID, &fh, currentFrame->pageData) == RC_OK) {
                        writeOperations++; // Track write operations
                    }
                }
            }
            
            // Replace frame pageData
            *currentFrame = *tgtPg;
            return;
        }
        
        // Move to next frame in FIFO manner
        currentIndex = (currentIndex + 1) % bufferCapacity;
        trialCount++;
    }}

extern void LFU(BM_BufferPool *const bp, PageFrame *tgtPg) {
    PageFrame *frames = (PageFrame *)(bp->mgmtData);
    int minFreqIdx = (lfuIndex % bufferCapacity);
    int curIdx = minFreqIdx, nextIdx, minFreqVal = INT_MAX, tries = 0;

    // Attempt to find a page with zero pinCount and initialize the least-frequent index
    while (tries < bufferCapacity) {
        nextIdx = (minFreqIdx + tries) % bufferCapacity;
        PageFrame *curFrame = &frames[nextIdx];
        if (curFrame->pinCount == 0) {
            minFreqIdx = nextIdx;
            minFreqVal = curFrame->referenceCount; // Track the frequency
            break; // Found an initial candidate page
        }
        tries++;
    }

    // Now, search for the least frequently used page among all eligible ones
    for (int i = 0; i < bufferCapacity; i++) {
        curIdx = (minFreqIdx + i) % bufferCapacity;
        PageFrame *curFrame = &frames[curIdx];

        if (curFrame->pinCount == 0 && curFrame->referenceCount < minFreqVal) {
            minFreqIdx = curIdx;
            minFreqVal = curFrame->referenceCount;
        }
    }

    // Declare selectedFrame here, outside of the for loop
    PageFrame *selectedFrame = &frames[minFreqIdx];

    // Check if the selected frame is dirty, and flush to disk if necessary
    if (selectedFrame->isDirty) {
        SM_FileHandle fh;
        if (openPageFile(bp->pageFile, &fh) == RC_OK) {
            if (writeBlock(selectedFrame->pageID, &fh, selectedFrame->pageData) == RC_OK) {
                writeOperations++; // Increment write count for successful write
                selectedFrame->isDirty = 0; // Reset dirty bit after writing
            }
        }
    }

    // Replace the chosen page with the target page's pageData
    *selectedFrame = *tgtPg;

    // Update the LFU pointer to the next frame after the replacement
    lfuIndex = (minFreqIdx + 1) % bufferCapacity;
}

extern void LRU(BM_BufferPool *const bufferPool, PageFrame *targetPage) {
    PageFrame *pages = (PageFrame *)bufferPool->mgmtData;
    int lruIdx = -1;
    int leastRecent = INT_MAX;

    // Search for the least recently used page (LRU) with pinCount == 0
    for (int idx = 0; idx < bufferCapacity; idx++) {
        if (pages[idx].pinCount == 0 && pages[idx].accessCount < leastRecent) {
            lruIdx = idx;
            leastRecent = pages[idx].accessCount;
        }
    }

    // If no page is eligible for replacement, return early
    if (lruIdx == -1) return;

    // Write the evicted dirty page back to disk (if necessary)
    if (pages[lruIdx].isDirty) {
        SM_FileHandle fileHandle;
        if (openPageFile(bufferPool->pageFile, &fileHandle) == RC_OK) {
            if (writeBlock(pages[lruIdx].pageID, &fileHandle, pages[lruIdx].pageData) == RC_OK) {
                writeOperations++;  // Increment write operations
            }
        }
    }

    // Replace the LRU page with the target page
    pages[lruIdx] = *targetPage;  // Efficient assignment instead of manual copying of each field

    // Increment cachehits to reflect the most recent access
    pages[lruIdx].accessCount = ++cachehits;  // Increment global cachehits counter
}




extern void CLOCK(BM_BufferPool *const bp, PageFrame *tgtPg) {
    PageFrame *frames = (PageFrame *)bp->mgmtData;

    while (1) {
        // Ensure clockHand stays within buffer range
        clockHand %= bufferCapacity;

        // If the page is unpinned and its reference bit is 0, it's the best candidate for replacement
        if (frames[clockHand].pinCount == 0) {
            if (frames[clockHand].accessCount == 0) {
                // Flush the page if it's dirty before replacement
                if (frames[clockHand].isDirty) {
                    SM_FileHandle fh;
                    RC rc = openPageFile(bp->pageFile, &fh);
                    if (rc == RC_OK) {
                        rc = writeBlock(frames[clockHand].pageID, &fh, frames[clockHand].pageData);
                        if (rc == RC_OK) {
                            writeOperations++; // Track successful writes
                        } else {
                            printf("Error: Failed to write dirty page %d to disk.\n", frames[clockHand].pageID);
                        }
                    } else {
                        printf("Error: Failed to open page file %s.\n", bp->pageFile);
                    }
                }

                // Replace the page with new page pageData
                frames[clockHand] = *tgtPg;

                // Reset reference bit for future access
                frames[clockHand].accessCount = 1; 

                // Move clock pointer for next replacement
                clockHand = (clockHand + 1) % bufferCapacity;
                return;
            } else {
                // If reference bit is 1, give it a second chance and reset it
                frames[clockHand].accessCount = 0;
            }
        }

        // Move clock pointer forward
        clockHand = (clockHand + 1) % bufferCapacity;
    }
}




extern RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
                         const int numPages, ReplacementStrategy strategy,
                         void *stratData) {
    // Set initial values for buffer pool
    bm->pageFile = (char *)pageFileName;  // Store the page file name
    bm->numPages = numPages;              // Set the number of pages
    bm->strategy = strategy;              // Set the replacement strategy

    // Allocate memory for page frames
    PageFrame *pages = (PageFrame *)malloc(sizeof(PageFrame) * numPages);
    if (pages == NULL) {
        return RC_MEMORY_ALLOCATION_ERROR; // Return error code for memory allocation failure
    }

    // Initialize the global buffer size
    bufferCapacity = numPages;

    // Use memset to zero-initialize page frames (clear all values)
    memset(pages, 0, sizeof(PageFrame) * numPages);

    // Set additional initial values for each page frame
    for (int i = 0; i < numPages; i++) {
        pages[i].pageID = -1;          // No page is loaded initially
        pages[i].accessCount = 0;      // Reset the cachehits count for all pages
        pages[i].referenceCount = 0;   // Reset reference number for all pages
        pages[i].pinCount = 0;         // All pages are unpinned initially
        pages[i].isDirty = 0;          // No dirty pages initially
    }

    // Assign the allocated pages to the buffer pool's management pageData
    bm->mgmtData = pages;

    // Initialize the buffer pool-related global variables
    writeOperations = 0;  // Reset the write count
    clockHand = 0;        // Reset clock pointer for CLOCK strategy (if used)
    lfuIndex = 0;         // Reset LFU pointer for LFU strategy (if used)

    return RC_OK;  // Return success if all steps completed successfully
}

extern RC shutdownBufferPool(BM_BufferPool *const bm) {
    // Retrieve the page frames from the buffer pool
    PageFrame *frameSet = (PageFrame *)bm->mgmtData;

    // Ensure all dirty pages are written back to disk
    forceFlushPool(bm);

    // Check if there are any pinned pages
    for (int idx = 0; idx < bm->numPages; idx++) {
        if (frameSet[idx].pinCount > 0) {
            // Return error if any page is still pinned
            return RC_PINNED_PAGES_IN_BUFFER;
        }
    }

    // Free memory for the page frames
    free(frameSet);

    // Set the management pageData pointer to NULL to safely release it
    bm->mgmtData = NULL;

    // Return success if no issues were found
    return RC_OK;
}


extern RC forceFlushPool(BM_BufferPool *const bm) {
    PageFrame *pageFrames = (PageFrame *)bm->mgmtData; // Pointer to the buffer pool's page frames
    SM_FileHandle fh;

    // Open the page file once before iterating over frames
    if (openPageFile(bm->pageFile, &fh) != RC_OK) {
        // Return an existing error code if the file cannot be opened
        return RC_FILE_NOT_FOUND; // Use an existing error code like RC_FILE_NOT_FOUND
    }

    // Iterate over the frames in the buffer pool
    for (int idx = 0; idx < bm->numPages; idx++) {
        PageFrame *currentFrame = &pageFrames[idx]; // Reference the current page frame
        
        // Check if the frame is dirty and not pinned
        if (currentFrame->pinCount == 0 && currentFrame->isDirty == 1) {
            // Write the dirty page to the file if it's not pinned
            if (writeBlock(currentFrame->pageID, &fh, currentFrame->pageData) == RC_OK) {
                // Reset dirty bit after successful write
                currentFrame->isDirty = 0;
                writeOperations++; // Increment the write operation count
            } else {
                // Optionally handle write errors (e.g., logging)
            }
        }
    }

    // Optionally close the file handle if required
    closePageFile(&fh);

    return RC_OK; // Indicate successful flush
}


extern RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page) {
    // Retrieve the page frames from the buffer pool
    PageFrame *pageFrames = (PageFrame *)bm->mgmtData;

    // Iterate over the frames in the buffer pool
    for (int idx = 0; idx < bm->numPages; idx++) {
        // Check if the current frame matches the page number
        if (pageFrames[idx].pageID == page->pageNum) {
            pageFrames[idx].isDirty = 1; // Mark the page as dirty
            return RC_OK; // Return success once the page is marked as dirty
        }
    }

    // Return an error if the page is not found in the buffer pool
    return RC_ERROR;
}


extern RC unpinPage(BM_BufferPool *const bufferMgr, BM_PageHandle *const page) {
    PageFrame *frames = (PageFrame *)bufferMgr->mgmtData;

    // Iterate through the frames in the buffer pool
    for (int idx = 0; idx < bufferCapacity; idx++) {
        // Check if the current page number matches the requested page
        if (frames[idx].pageID == page->pageNum) {
            // Decrease the pinCount only if the page is found
            if (frames[idx].pinCount > 0) {
                frames[idx].pinCount--;
            }
            return RC_OK; // Successfully unpinned the page
        }
    }

    // If no matching page is found, return a generic error code
    return RC_ERROR; // Use RC_ERROR instead of RC_PAGE_NOT_FOUND
}


extern RC forcePage(BM_BufferPool *const bufferMgr, BM_PageHandle *const page) {
    PageFrame *frames = (PageFrame *)bufferMgr->mgmtData;
    SM_FileHandle fileHandle;

    // Open the page file once before processing the frames
    RC openResult = openPageFile(bufferMgr->pageFile, &fileHandle);
    if (openResult != RC_OK) {
        return RC_FILE_NOT_FOUND; // Return error if file cannot be opened
    }

    // Iterate through the frames in the buffer pool
    for (int idx = 0; idx < bufferCapacity; idx++) {
        // Check if the current page matches the requested page
        if (frames[idx].pageID == page->pageNum) {
            // Perform the write operation to flush the page to disk
            if (writeBlock(frames[idx].pageID, &fileHandle, frames[idx].pageData) == RC_OK) {
                // Reset the dirty bit and increment the write count if the write was successful
                frames[idx].isDirty = 0;
                writeOperations++;
                break; // Exit the loop once the page is flushed
            } else {
                // Handle write error, if necessary (e.g., logging)
                return RC_WRITE_FAILED; // You can define RC_WRITE_FAILED if not already defined
            }
        }
    }

    // Close the file handle if necessary (optional, depending on your design)
    closePageFile(&fileHandle);

    return RC_OK; // Indicate success
}

extern RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageID) {
    PageFrame *bufferPool = (PageFrame *)bm->mgmtData;
    bool isBufFull = true;

    // Initialize the first buffer frame if uninitialized
    if (bufferPool[0].pageID == -1) {
        // Declare fileHandle locally to manage file operations within the scope
        SM_FileHandle fileHandle;
        openPageFile(bm->pageFile, &fileHandle);

        // Allocate memory for page data and handle memory allocation failure
        bufferPool[0].pageData = (SM_PageHandle)malloc(PAGE_SIZE);
        if (bufferPool[0].pageData == NULL) {
            return RC_MEMORY_ALLOCATION_ERROR;  // Handle memory allocation failure
        }

        // Ensure capacity for the requested page
        ensureCapacity(pageID, &fileHandle);

        // Read the page data into the buffer
        readBlock(pageID, &fileHandle, bufferPool[0].pageData);

        // Initialize buffer frame attributes
        bufferPool[0].pageID = pageID;
        bufferPool[0].pinCount = 1;
        bufferPool[0].accessCount = cachehits = 0;  // Initialize cachehits if needed
        bufferPool[0].referenceCount = 0;

        // Update the provided page structure
        page->pageNum = pageID;
        page->data = bufferPool[0].pageData;

        tailIndex = 0;  // Set the tail index for the buffer pool

        return RC_OK;  // Return success after initializing the first buffer frame
    }


    // Search for the page in the buffer pool or find an empty slot
    for (int i = 0; i < bufferCapacity; i++) {
        if (bufferPool[i].pageID == pageID) {
            // Page found in the buffer pool
            bufferPool[i].pinCount++;
            cachehits++;
            isBufFull = false;

            // Update strategy-specific metadata
            if (bm->strategy == RS_CLOCK)
                bufferPool[i].accessCount = 1;
            else if (bm->strategy == RS_LRU)
                bufferPool[i].accessCount = cachehits;
            else if (bm->strategy == RS_LFU)
                bufferPool[i].referenceCount++;
            else return 0;

            page->pageNum = pageID;
            page->data = bufferPool[i].pageData;

            clockHand++;
            return RC_OK;
        } else if (bufferPool[i].pageID == -1) {
    // Empty slot found, load the new page
    SM_FileHandle fileHandle;
    openPageFile(bm->pageFile, &fileHandle);

    // Allocate and read the block only once if it's not already done
    if (bufferPool[i].pageData == NULL) {
        bufferPool[i].pageData = (SM_PageHandle)malloc(PAGE_SIZE);
        if (bufferPool[i].pageData == NULL) {
            return RC_MEMORY_ALLOCATION_ERROR;  // Handle memory allocation failure
        }

        // Read the block into the allocated memory
        readBlock(pageID, &fileHandle, bufferPool[i].pageData);
    }

    // Update bufferPool attributes
    bufferPool[i].pageID = pageID;
    bufferPool[i].pinCount = 1;
    bufferPool[i].referenceCount = 0;

    // Increment counters
    tailIndex++;
    cachehits++;

    // Set access count based on strategy
    bufferPool[i].accessCount = (bm->strategy == RS_LRU) ? cachehits : 1;

    // Update the provided page structure
    page->pageNum = pageID;
    page->data = bufferPool[i].pageData;

    // Set flag indicating buffer is no longer full
    isBufFull = false;

    return RC_OK;
}

    }

    // If no empty slot is found, apply a replacement strategy
    if (isBufFull) {
    // Allocate memory for a new PageFrame and check for allocation failure
    PageFrame *newPage = (PageFrame *)malloc(sizeof(PageFrame));
    if (newPage == NULL) {
        return RC_MEMORY_ALLOCATION_ERROR;  // Handle memory allocation failure
    }

    SM_FileHandle fileHandle;
    openPageFile(bm->pageFile, &fileHandle);

    // Allocate memory for the page data and check for allocation failure
    newPage->pageData = (SM_PageHandle)malloc(PAGE_SIZE);
    if (newPage->pageData == NULL) {
        free(newPage);  // Free previously allocated memory before returning
        return RC_MEMORY_ALLOCATION_ERROR;  // Handle memory allocation failure
    }

    // Read the block into the new page's data
    readBlock(pageID, &fileHandle, newPage->pageData);

    // Initialize newPage attributes
    newPage->pageID = pageID;
    newPage->isDirty = 0;
    newPage->pinCount = 1;
    newPage->referenceCount = 0;

    // Increment counters and set access count based on strategy
    tailIndex++;
    cachehits++;
    newPage->accessCount = (bm->strategy == RS_LRU) ? cachehits : 1;

    // Update the provided page structure
    page->pageNum = pageID;
    page->data = newPage->pageData;

    // Apply the replacement strategy
    switch (bm->strategy) {
        case RS_FIFO:
            FIFO(bm, newPage);
            break;
        case RS_LRU:
            LRU(bm, newPage);
            break;
        case RS_CLOCK:
            CLOCK(bm, newPage);
            break;
        case RS_LFU:
            LFU(bm, newPage);
            break;
        case RS_LRU_K:
            printf("\nLRU-k algorithm not implemented\n");
            break;
        default:
            printf("\nAlgorithm Not Implemented\n");
            break;
    }
}

return RC_OK;  // Return success after completing operations
}

extern PageNumber *getFrameContents(BM_BufferPool *const bm) {
    // Allocate memory for frameContents dynamically based on bufferCapacity
    PageNumber *frameContents = (PageNumber *) malloc(sizeof(PageNumber) * bufferCapacity);
    if (!frameContents) {
        return NULL; // Return NULL if memory allocation fails
    }

    // Cast the management pageData to PageFrame array
    PageFrame *pageFrame = (PageFrame *) bm->mgmtData;

    // Loop through the buffer pool and store page numbers, or NO_PAGE for unused frames
    for (int i = 0; i < bufferCapacity; i++) {
        // Use a pointer to access the current page frame
        PageFrame *currentPage = &pageFrame[i];
        
        // Assign pageID or NO_PAGE to frameContents based on validity
        frameContents[i] = (currentPage->pageID != -1) ? currentPage->pageID : NO_PAGE;
    }

    return frameContents; // Return the frame contents array
}


extern bool *getDirtyFlags(BM_BufferPool *const bm) {
    bool *dirtyFlags = (bool *)malloc(sizeof(bool) * bm->numPages);
    if (!dirtyFlags) {
        return NULL;  // Return NULL if memory allocation fails
    }

    PageFrame *pageFrame = (PageFrame *)bm->mgmtData;
    for (int i = 0; i < bm->numPages; i++) {
        dirtyFlags[i] = pageFrame[i].isDirty;
    }

    return dirtyFlags;  // Return the dirty flags array
}



extern int *getFixCounts(BM_BufferPool *const bm) {
    // Allocate memory for fixCounts array based on bufferCapacity
    int *fixCounts = malloc(sizeof(int) * bufferCapacity);
    if (fixCounts == NULL) {
        return NULL;  // Return NULL if memory allocation fails
    }

    // Cast the management pageData to PageFrame array
    PageFrame *pageFrame = (PageFrame *)bm->mgmtData;

    // Iterate through the buffer pool to get the pinCount of each page
    // Iterate through the buffer pool to get the pinCount of each page
for (int i = 0; i < bufferCapacity; i++) {
    // Use a pointer to directly access the pinCount field
    PageFrame *currentPage = &pageFrame[i];
    fixCounts[i] = currentPage->pinCount;
}
return fixCounts;  // Return the fixCounts array
}


extern int getNumReadIO(BM_BufferPool *const bm) {
    return tailIndex >= 0 ? tailIndex + 1 : 0;
}


extern int getNumWriteIO(BM_BufferPool *const bm) {
    // Return the number of write IO operations
    // Assumes that writeOperations tracks the number of write IOs
    return writeOperations;
}
