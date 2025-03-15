#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "storage_mgr.h"


FILE *PageFile;


void initStorageManager(void){
    PageFile=NULL;              // Initializing the file pointer to NULL to ensure it starts in a clean, uninitialized state.
}



RC createPageFile(char *file_path) {
    // Try to open the file for writing. If it doesn't exist, it gets created.
    FILE *fileObj = fopen(file_path, "wb+");
    if (fileObj == NULL) {
        return RC_FILE_NOT_FOUND;  // Return an error if the file can't be opened
    }

    // Allocate space for an empty page (all zeros)
    char *emptypage = (char *)calloc(PAGE_SIZE, sizeof(char));
    if (emptypage == NULL) {
        fclose(fileObj);  // Close the file if memory allocation fails
        return RC_ERROR;  // Return an error if we couldn't get memory
    }

    // Set the empty page to zeros (not really needed since calloc does this)
    memset(emptypage, 0, PAGE_SIZE); 

    // Write the empty page to the file
    size_t WrittenBytes = fwrite(emptypage, sizeof(char), PAGE_SIZE, fileObj);
    if (WrittenBytes < PAGE_SIZE) {
        free(emptypage);  // Free the memory if writing fails
        fclose(fileObj);  // Close the file
        return RC_WRITE_FAILED;  // Return an error if we couldn't write all the data
    }

    // Print a success message after everything went fine
    printf("Successfully created the file\n");

    // Clean up by freeing the memory and closing the file
    free(emptypage);
    fclose(fileObj);

    return RC_OK;  // Return success
}



RC openPageFile(char *filePath, SM_FileHandle *fileHandle) {
    // Try to open the file for reading.
    FILE *filePtr = fopen(filePath, "r");
    
    // Check if the file opened successfully.
    if (!filePtr) {
        printError(RC_FILE_NOT_FOUND);  // If not, print an error and return failure.
        return RC_FILE_NOT_FOUND;
    }
    
    // Set up the file handle with the file name and starting page position.
    fileHandle->fileName = filePath;
    fileHandle->curPagePos = 0;  // Start at the first page of the file.
    
    // Find the size of the file by seeking to the end and getting the position.
    fseek(filePtr, 0L, SEEK_END);  // Move to the end of the file.
    long fileSize = ftell(filePtr); // Get the current position (which is the file size).
    rewind(filePtr); // Go back to the beginning of the file for future reads.
    
    // Calculate how many pages the file has and update the file handle.
    // The formula ensures any leftover data that doesn't fill a full page still counts as a full page.
    fileHandle->totalNumPages = (fileSize + PAGE_SIZE - 1) / PAGE_SIZE;
    
    // Close the file since we don’t need it anymore.
    fclose(filePtr);
    return RC_OK;  // Return success after everything is set up.
}


RC closePageFile(SM_FileHandle *fileHandle) {
    // Check if the file handle is valid. If not, print an error and return failure.
    if (!fileHandle) {
        printError(RC_FILE_HANDLE_NOT_INIT);
        return RC_FILE_HANDLE_NOT_INIT;  // Return error if the file handle is not initialized.
    }

    // Reset the file handle by clearing the file name and resetting other variables.
    fileHandle->fileName = "";  // Clear the file name.
    fileHandle->totalNumPages = 0;  // Reset total number of pages.
    fileHandle->curPagePos = 0;  // Reset the current page position.

    return RC_OK;  // Return success after closing the file handle.
}



RC destroyPageFile(char *fileName) {
    // Try to open the file in read/write mode.
    FILE *f1 = fopen(fileName, "r+"); 
    
    // Check if the file was opened successfully.
    if (f1 != NULL) {
        fclose(f1);  // Close the file since we just needed to check if it exists.

        // Attempt to delete the file.
        int result = remove(fileName); 
        
        // If we couldn't delete the file, return an error.
        if (result != 0) {
            return RC_FILE_NOT_FOUND;
        }
        return RC_OK;  // Return success if the file was deleted.
    } 
    else {
        // If the file doesn't exist, return an error.
        return RC_FILE_NOT_FOUND;
    }
}


// ***************************SECOND PHASE OF FUNCTIONS**********************************

RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // Check if the file handle or memory page is null. If so, return an error.
    if (!fHandle || !memPage) {
        return RC_ERROR;
    }

    // Ensure the requested page number is valid (within bounds).
    if (pageNum < 0 || pageNum >= fHandle->totalNumPages) {
        return RC_READ_NON_EXISTING_PAGE;
    }

    // Try to open the file in read mode.
    FILE *fileObj = fopen(fHandle->fileName, "r");
    if (!fileObj) {
        return RC_FILE_NOT_FOUND;  // Return error if the file can’t be opened.
    }

    // Calculate the position of the page in the file.
    long offset = (long)pageNum * PAGE_SIZE;
    
    // Move the file pointer to the calculated page position.
    if (fseek(fileObj, offset, SEEK_SET) != 0) {
        fclose(fileObj);  // Close the file if there's an error with the file position.
        return RC_READ_NON_EXISTING_PAGE;  // Return an error if seeking failed.
    }

    // Read the data into memory, chunk by chunk, until the entire page is read.
    size_t bytesRead = 0;
    while (bytesRead < PAGE_SIZE) {
        size_t chunk = fread(memPage + bytesRead, sizeof(char), PAGE_SIZE - bytesRead, fileObj);
        if (chunk == 0) {
            fclose(fileObj);  // Close the file if reading fails.
            return RC_ERROR;  // Return error if we can't read the data.
        }
        bytesRead += chunk;  // Update how many bytes have been read.
    }

    // Update the current page position in the file handle.
    fHandle->curPagePos = pageNum;
    fclose(fileObj);  // Close the file after reading is complete.
    
    return RC_OK;  // Return success when everything goes smoothly.
}



int getBlockPos(SM_FileHandle *fHandle) {
    // Check if the file handle is valid. If not, return -1 to indicate an error.
    if (fHandle == NULL) {
        return -1;  // Invalid file handle, so return an error.
    }
    // Return the current page position stored in the file handle.
    return fHandle->curPagePos;
}

RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // Check if the file handle or memory page is null. If so, return an error.
    if (!fHandle || !memPage) {
        return RC_ERROR;
    }

    // Try to open the file in read mode.
    FILE *pageFile = fopen(fHandle->fileName, "r");
    if (!pageFile) {
        return RC_FILE_NOT_FOUND;  // Return error if the file can't be opened.
    }

    // Move the file pointer to the beginning of the file (first block).
    if (fseek(pageFile, 0, SEEK_SET) != 0) {
        fclose(pageFile);  // Close the file if seeking fails.
        return RC_READ_NON_EXISTING_PAGE;  // Return error if seeking failed.
    }

    // Read the first page of data into memory.
    size_t bytesRead = fread(memPage, sizeof(char), PAGE_SIZE, pageFile);
    if (bytesRead < PAGE_SIZE) {
        fclose(pageFile);  // Close the file if reading fails.
        return (feof(pageFile)) ? RC_READ_NON_EXISTING_PAGE : RC_ERROR;  // Handle file end or read error.
    }

    // Update the current page position to the first page.
    fHandle->curPagePos = 0;
    fclose(pageFile);  // Close the file after reading.

    return RC_OK;  // Return success if everything went well.
}

RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // Check if the file handle or memory page is null. If so, return an error.
    if (fHandle == NULL || memPage == NULL) {
        return RC_ERROR;
    }

    // Make sure we're not at the beginning of the file (there's a previous block).
    if (fHandle->curPagePos <= 0) {
        return RC_READ_NON_EXISTING_PAGE;  // Can't go back if we're already at the first page.
    }

    // Try to open the file in read mode.
    FILE *pageFile = fopen(fHandle->fileName, "r");
    if (pageFile == NULL) {
        return RC_FILE_NOT_FOUND;  // Return error if the file can't be opened.
    }

    // Calculate the position of the previous page.
    long prevPagePos = (long)fHandle->curPagePos - PAGE_SIZE;

    // Move the file pointer to the previous page position.
    if (fseek(pageFile, prevPagePos, SEEK_SET) != 0) {
        fclose(pageFile);  // Close the file if seeking fails.
        return RC_READ_NON_EXISTING_PAGE;  // Return error if seeking failed.
    }

    // Read the previous page of data into memory.
    size_t bytesRead = 0;
    while (bytesRead < PAGE_SIZE) {
        size_t chunk = fread(memPage + bytesRead, sizeof(char), PAGE_SIZE - bytesRead, pageFile);
        if (chunk == 0) {
            fclose(pageFile);  // Close the file if reading fails.
            return RC_ERROR;  // Return error if we couldn't read any data.
        }
        bytesRead += chunk;  // Update the number of bytes read.
    }

    // Update the current page position to the previous page.
    fHandle->curPagePos = prevPagePos;
    fclose(pageFile);  // Close the file after reading.

    return RC_OK;  // Return success if everything went well.
}

RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // Check if the file handle or memory page is null. If so, return an error.
    if (fHandle == NULL || memPage == NULL) {
        return RC_ERROR;
    }

    // Try to open the file in read mode.
    FILE *pageFile = fopen(fHandle->fileName, "r");
    if (pageFile == NULL) {
        return RC_FILE_NOT_FOUND;  // Return error if the file can't be opened.
    }

    // Move the file pointer to the current page position.
    if (fseek(pageFile, fHandle->curPagePos, SEEK_SET) != 0) {
        fclose(pageFile);  // Close the file if seeking fails.
        return RC_READ_NON_EXISTING_PAGE;  // Return error if seeking failed.
    }

    // Read the current page of data into memory.
    size_t bytesRead = fread(memPage, sizeof(char), PAGE_SIZE, pageFile);
    if (bytesRead < PAGE_SIZE) {
        fclose(pageFile);  // Close the file if reading fails.
        return (feof(pageFile)) ? RC_READ_NON_EXISTING_PAGE : RC_ERROR;  // Handle file end or read error.
    }

    // Update the current page position to the next page.
    fHandle->curPagePos += PAGE_SIZE;
    fclose(pageFile);  // Close the file after reading.

    return RC_OK;  // Return success if everything went well.
}


RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // Make sure the file handle and memory page are not null.
    if (fHandle == NULL || memPage == NULL) {
        return RC_ERROR;
    }

    // Check if we're already at the last page (can't go forward).
    if (fHandle->curPagePos / PAGE_SIZE >= fHandle->totalNumPages - 1) {
        return RC_READ_NON_EXISTING_PAGE;  // No more pages ahead.
    }

    // Open the file to read.
    FILE *pageFile = fopen(fHandle->fileName, "rb");
    if (pageFile == NULL) {
        return RC_FILE_NOT_FOUND;  // File not found, return error.
    }

    // Move to the next page position.
    long startPosition = (long)fHandle->curPagePos + PAGE_SIZE;

    // Seek to that position in the file.
    if (fseek(pageFile, startPosition, SEEK_SET) != 0) {
        fclose(pageFile);  // If we can't seek, close the file and return error.
        return RC_READ_NON_EXISTING_PAGE;
    }

    // Read the next page into memory.
    size_t bytesRead = 0;
    while (bytesRead < PAGE_SIZE) {
        size_t chunk = fread(memPage + bytesRead, sizeof(char), PAGE_SIZE - bytesRead, pageFile);
        if (chunk == 0) {
            fclose(pageFile);  // If no data is read, return error.
            return feof(pageFile) ? RC_READ_NON_EXISTING_PAGE : RC_ERROR;
        }
        bytesRead += chunk;  // Keep track of how much we’ve read.
    }

    // Update the current page position.
    fHandle->curPagePos = startPosition;
    fclose(pageFile);  // Close the file after reading.

    return RC_OK;  // All good!
}

RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // First, check that we got valid inputs.
    if (fHandle == NULL || memPage == NULL) {
        return RC_ERROR;  // Return error if inputs are bad.
    }

    // Make sure the file has pages in it.
    if (fHandle->totalNumPages <= 0) {
        return RC_READ_NON_EXISTING_PAGE;  // No pages, so can't read.
    }

    // Calculate where the last page is in the file.
    int startPosition = (fHandle->totalNumPages - 1) * PAGE_SIZE;

    // Open the file to read in binary mode.
    FILE *pageFile = fopen(fHandle->fileName, "rb");
    if (pageFile == NULL) {
        return RC_FILE_NOT_FOUND;  // If file doesn't exist, return error.
    }

    // Move the file pointer to the last page.
    if (fseek(pageFile, startPosition, SEEK_SET) != 0) {
        fclose(pageFile);  // If seeking fails, close the file and return error.
        return RC_READ_NON_EXISTING_PAGE;
    }

    // Read the last page into the memory buffer.
    if (fread(memPage, sizeof(char), PAGE_SIZE, pageFile) != PAGE_SIZE) {
        fclose(pageFile);  // If we can't read the full page, return error.
        return RC_ERROR;
    }

    // Update the current page position to the last page.
    fHandle->curPagePos = startPosition;

    // Close the file after reading.
    fclose(pageFile);

    return RC_OK;  // Success!
}


RC writeBlock(int pageIndex, SM_FileHandle *fileStruct, SM_PageHandle pageData) {
    // Check if the page index is valid
    if (pageIndex < 0 || pageIndex >= fileStruct->totalNumPages) {
        return RC_WRITE_FAILED;  // Invalid index, can't write.
    }

    // Open the file in read/write mode
    FILE *filePtr = fopen(fileStruct->fileName, "r+");
    if (!filePtr) {
        return RC_FILE_NOT_FOUND;  // File not found, return error.
    }

    // Calculate where in the file we need to write
    long fileOffset = (long)pageIndex * PAGE_SIZE;
    
    // Move the file pointer to the right position
    if (fseek(filePtr, fileOffset, SEEK_SET) != 0) {
        fclose(filePtr);  // Couldn't move, close file and return error
        return RC_WRITE_FAILED;
    }
    
    // Write the page data to the file
    size_t bytesWritten = fwrite(pageData, sizeof(char), PAGE_SIZE, filePtr);
    if (bytesWritten < PAGE_SIZE) {
        fclose(filePtr);  // Didn't write enough, return error
        return RC_WRITE_FAILED;
    }
    
    // Update where we are in the file
    fileStruct->curPagePos = fileOffset;
    fclose(filePtr);

    return RC_OK;  // Everything went fine
}

RC writeCurrentBlock(SM_FileHandle *fileStruct, SM_PageHandle pageData) {
    // Check if the file structure and data are valid
    if (!fileStruct || !pageData) {
        return RC_ERROR;  // Invalid input, return error
    }

    // Open the file in read/write mode
    FILE *filePtr = fopen(fileStruct->fileName, "r+");
    if (!filePtr) {
        return RC_FILE_NOT_FOUND;  // Can't find the file, error
    }

    // If the file size isn't enough, add an empty block
    if (fileStruct->curPagePos >= fileStruct->totalNumPages * PAGE_SIZE) {
        appendEmptyBlock(fileStruct);  // Add an empty block if needed
    }

    // Move to the right place in the file to write
    if (fseek(filePtr, fileStruct->curPagePos, SEEK_SET) != 0) {
        fclose(filePtr);  // Couldn't move, return error
        return RC_WRITE_FAILED;
    }

    // Write the data, making sure we don't overflow
    size_t writeSize = (strlen(pageData) < PAGE_SIZE) ? strlen(pageData) : PAGE_SIZE;
    if (fwrite(pageData, sizeof(char), writeSize, filePtr) < writeSize) {
        fclose(filePtr);  // Couldn't write everything, return error
        return RC_WRITE_FAILED;
    }

    // Update where we are in the file
    fileStruct->curPagePos = ftell(filePtr);
    fclose(filePtr);

    return RC_OK;  // All done, success!
}


RC appendEmptyBlock(SM_FileHandle *fHandle) {
    // Allocate memory for an empty page
    SM_PageHandle emptyPage = (SM_PageHandle)calloc(PAGE_SIZE, sizeof(char));
    if (emptyPage == NULL) return RC_WRITE_FAILED;  // Failed to allocate memory

    // Open the file in append mode to add a new block
    FILE *file = fopen(fHandle->fileName, "a");
    if (file == NULL) {
        free(emptyPage);  // Free memory before returning error
        return RC_FILE_NOT_FOUND;  // Couldn't find the file
    }

    // Write the empty page to the file
    if (fwrite(emptyPage, sizeof(char), PAGE_SIZE, file) < PAGE_SIZE) {
        fclose(file);  // Close file before returning error
        free(emptyPage);  // Clean up memory
        return RC_WRITE_FAILED;  // Writing failed
    }

    // Close the file and clean up memory
    fclose(file);
    free(emptyPage);

    // Update the total number of pages in the file
    fHandle->totalNumPages++;

    return RC_OK;  // Everything went fine
}

RC ensureCapacity(int requiredPages, SM_FileHandle *handle) {
    // Check if we already have enough pages
    if (handle->totalNumPages >= requiredPages) return RC_OK;  // No need to add pages

    // Open the file in read/write mode
    FILE *fileAccess = fopen(handle->fileName, "r+b");
    if (fileAccess == NULL) {
        // File doesn't exist, so open in append mode to create it
        fileAccess = fopen(handle->fileName, "a+");
        if (fileAccess == NULL) return RC_FILE_NOT_FOUND;  // Still can't open the file
    }

    // Calculate how many pages we need to add
    int pagesToAdd = requiredPages - handle->totalNumPages;

    // Add the required number of empty pages
    for (int i = 0; i < pagesToAdd; i++) {
        RC appendStatus = appendEmptyBlock(handle);  // Add an empty page
        if (appendStatus != RC_OK) {
            fclose(fileAccess);  // Close the file before returning error
            return appendStatus;  // Return the error from append
        }
    }

    // Close the file after we're done
    fclose(fileAccess);
    return RC_OK;  // Successfully ensured capacity
}
