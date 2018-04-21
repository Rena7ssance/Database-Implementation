

/****************************************************
** COPYRIGHT 2016, Chris Jermaine, Rice University **
**                                                 **
** The MyDB Database System, COMP 530              **
** Note that this file contains SOLUTION CODE for  **
** A1.  You should not be looking at this file     **
** unless you have completed A1!                   **
****************************************************/


#ifndef BUFFER_MGR_H
#define BUFFER_MGR_H

#include "CheckLRU.h"
#include "Lock.h"
#include <map>
#include <memory>
#include "MyDB_Page.h"
#include "MyDB_PageHandle.h"
#include "MyDB_Table.h"
#include "PageCompare.h"
#include <queue>
#include "TableCompare.h"
#include <set>

using namespace std;

class MyDB_BufferManager;
typedef shared_ptr <MyDB_BufferManager> MyDB_BufferManagerPtr;

class MyDB_BufferManager {

public:

	// this routine starts up a set of threads.  The number of threads started
	// is equal to the number of items in the param set args.  Each thread begins
	// its life inside the function start_routine; start_routine is invoked with
	// one of the items in the param set args (so, for example, the first thread
	// is invoked with args[0], the second with args[1], and so on.  Note that the
	// threads created are pthreads, so any synchronization should be implemented
	// using pthreads synchroniztion primitives.  Also note that the buffer manager
	// is thread safe with respect to these threads.  That is, the various threads
	// created can all access pages from the buffer manager simultaneously without
	// worry.  When all of the threads have exited from start_routine, then the
	// call to executeThreads returns.
	void executeThreads (void (*start_routine) (void *), vector <void *> args);

	// returns an object that locks the buffer manager... the buffer manager stays
	// locked as long as this object exists.  This SHOULD GENERALLY NOT BE CALLED
	// BY APPLICATIONS AS THE INTERFACE TO THE BUFFER MANAGER IS THREAD SAFE.  It
	// is typically called internally, by the buffer manager and associated classes
	pthread_mutex_t *getLock ();

	// gets the i^th page in the table whichTable... note that if the page
	// is currently being used (that is, the page is current buffered) a handle 
	// to that already-buffered page should be returned
	MyDB_PageHandle getPage (MyDB_TablePtr whichTable, long i);

	// gets a temporary page that will no longer exist (1) after the buffer manager
	// has been destroyed, or (2) there are no more references to it anywhere in the
	// program.  Typically such a temporary page will be used as buffer memory.
	// since it is just a temp page, it is not associated with any particular 
	// table
	MyDB_PageHandle getPage ();

	// gets the i^th page in the table whichTable... the only difference 
	// between this method and getPage (whicTable, i) is that the page will be 
	// pinned in RAM; it cannot be written out to the file... note that in Chris'
	// implementation, a request for a pinned page that is made when the buffer
	// is ENTIRELY full of pinned pages will return a nullptr
	MyDB_PageHandle getPinnedPage (MyDB_TablePtr whichTable, long i);

	// gets a temporary page, like getPage (), except that this one is pinned
	MyDB_PageHandle getPinnedPage ();

	// un-pins the specified page
	void unpin (MyDB_PagePtr unpinMe);

	// creates an LRU buffer manager... params are as follows:
	// 1) the size of each page is pageSize 
	// 2) the number of pages managed by the buffer manager is numPages;
	// 3) temporary pages are written to the file tempFile
	MyDB_BufferManager (size_t pageSize, size_t numPages, string tempFile);
	
	// when the buffer manager is destroyed, all of the dirty pages need to be
	// written back to disk, and any temporary files need to be deleted
	~MyDB_BufferManager ();

	// returns the page size
	size_t getPageSize ();
	
private:

	// tells us the LRU number of each of the pages
	set <MyDB_PagePtr, CheckLRU> lastUsed;

	// list of ALL of the page objects that are currently in existence
	map <pair <MyDB_TablePtr, size_t>, MyDB_PagePtr, PageCompare> allPages;
	
	// lists the FDs for all of the files
	map <MyDB_TablePtr, int, TableCompare> fds;

	// all of the chunks of RAM that are currently not allocated
	vector <void *> availableRam;

	// all of the positions in the temporary file that are currently not in use
	priority_queue<size_t, vector<size_t>, greater<size_t>> availablePositions;

	// the page size
	size_t pageSize;

	// the time tick associated with the MRU page
	long lastTimeTick;

	// the last position in the temporary file
	size_t lastTempPos;

	// where we write the data
	string tempFile;

	// the number of buffer pages
	size_t numPages;

	// when running multi-threaded, each thread automatically pins one page (the
	// last one accessed) so that we are guaranteed that it can't be expelled... this
	// lists all of the pages of RAM that have been pinned in this way
	vector <void *> threadPinnedPages;

	// this is the start and end of the call stack used to run each of the threads	
	// when we are executing in multi-thrreaded mode
	void *stackBase;
	void *stackEnd;

	// this is the lock for the buffer manager
	pthread_mutex_t myLock;

	// this tells the buffer manager that the current thread has recently accessed
	// the memory location indicated, and so the associated page cannot be expelled
	void setCannotExpell (void *setMe);

	// checks to see if THIS thread has already pinned this page by accessing
	bool checkIfThreadPinned (void *checkMe);

	// checks to see if any of the threads have recently accessed the memory location
	// so that it cannot be expelled from the buffer manager
	bool checkCannotExpell (void *checkMe);

	// so that the page can access these private methods
	friend class MyDB_Page;
	friend class SortMergeJoin;

	// kick out the LRU page
	void kickOutPage ();

	// process an access to the given page
	void access (MyDB_PagePtr updateMe);

	// removes all traces of the page from the buffer manager
	void killPage (MyDB_PagePtr killMe);

};

#endif


