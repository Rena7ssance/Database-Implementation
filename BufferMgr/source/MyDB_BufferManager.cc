

/****************************************************
** COPYRIGHT 2016, Chris Jermaine, Rice University **
**                                                 **
** The MyDB Database System, COMP 530              **
** Note that this file contains SOLUTION CODE for  **
** A1.  You should not be looking at this file     **
** unless you have completed A1!                   **
****************************************************/


#ifndef BUFFER_MGR_C
#define BUFFER_MGR_C

#include <fcntl.h>
#include <iostream>
#include "MyDB_BufferManager.h"
#include "MyDB_Page.h"
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include <utility>

using namespace std;

size_t MyDB_BufferManager :: getPageSize () {
	return pageSize;
}

MyDB_PageHandle MyDB_BufferManager :: getPage (MyDB_TablePtr whichTable, long i) {
		
	Lock temp (getLock ());

	// open the file, if it is not open
	if (fds.count (whichTable) == 0) {
		int fd = open (whichTable->getStorageLoc ().c_str (), O_CREAT | O_RDWR, 0666);
		fds[whichTable] = fd;
	}

	// make sure we don't have a null table
	if (whichTable == nullptr) {
		cout << "Can't allocate a page with a null table!!\n";
		exit (1);
	}
	
	// next, see if the page is already in existence
	pair <MyDB_TablePtr, long> whichPage = make_pair (whichTable, i);
	if (allPages.count (whichPage) == 0) {

		// it is not there, so create a page
		MyDB_PagePtr returnVal = make_shared <MyDB_Page> (whichTable, i, *this);
		allPages [whichPage] = returnVal;
		return make_shared <MyDB_PageHandleBase> (returnVal);
	}

	// it is there, so return it
	return make_shared <MyDB_PageHandleBase> (allPages [whichPage]);
}

MyDB_PageHandle MyDB_BufferManager :: getPage () {

	Lock temp (getLock ());

	// open the file, if it is not open
	if (fds.count (nullptr) == 0) {
		int fd = open (tempFile.c_str (), O_TRUNC | O_CREAT | O_RDWR, 0666);
		fds[nullptr] = fd;
	}

	// check if we are extending the size of the temp file
	size_t pos;
	if (availablePositions.size () == 0) {
		pos = lastTempPos++;
	} else {
		pos = availablePositions.top ();
		availablePositions.pop ();
	}

	MyDB_PagePtr returnVal = make_shared <MyDB_Page> (nullptr, pos, *this);
	return make_shared <MyDB_PageHandleBase> (returnVal);
}

// this stores the info needed for the buffer manager to start up a thread
struct ThreadArg {
	void *param;
	void (*start_routine) (void *);
};

// called to actually start up a thread
void *startThread (void *arg) {
	ThreadArg *temp = (ThreadArg *) arg;
	temp->start_routine (temp->param);
	delete temp;		
	return nullptr;
}

void MyDB_BufferManager :: executeThreads (void (*start_routine) (void *), vector <void *> args) {

	// will store all of the threads
	vector <pthread_t> threads;

	// each thread has one pinned page (the last one executed)... reset this
	threadPinnedPages.resize (args.size () + 1);

	// each of the threads lacks a thread pinned page
	for (int i = 0; i < args.size (); i++) {
		threadPinnedPages[i + 1] = nullptr;
	}

	// this is the location where each worker is going to have their stack
	void *origStackBase = malloc (1024 * 1024 * 4 * (args.size () + 2));

	// align it to 2^22... chop off the last 22 bits, and then add 2^22 to the address
	stackBase = (void *) (((((size_t) origStackBase) >> 22) << 22) + (1024 * 1024 * 4));
	void *nextPos  = stackBase;
	stackEnd = ((char *) stackBase) + 1024 * 1024 * 4 * args.size ();

	// create all of the threads
	for (auto arg : args) {

		pthread_t thread;
		threads.push_back(thread);

		// adjust the stack base to align it correctly
		void *stackBaseIn = (void *) ((((long) nextPos + (PTHREAD_STACK_MIN - 1)) /
			PTHREAD_STACK_MIN) * PTHREAD_STACK_MIN);
	
		// this is the location of the stack base for the next thread
		nextPos = ((char *) nextPos) + (1024 * 1024 * 4);

		// create a thread
		pthread_attr_t tattr;
		pthread_attr_init (&tattr);
		pthread_attr_setstack (&tattr, stackBaseIn, ((char *) nextPos) - (char *) stackBaseIn);

		// this is the data
		ThreadArg *temp = new ThreadArg;
		temp->param = arg;
		temp->start_routine = start_routine;
		int return_code = pthread_create(&(threads[threads.size() - 1]), &tattr, startThread, temp);
		pthread_attr_destroy (&tattr);

		if (return_code) {
			cout << "ERROR; return code from pthread_create () is " << return_code << '\n';
			exit(-1);
		}
	}
	
	// and wait util they all complete
	for (auto thread : threads) {
		pthread_join (thread, nullptr);
	}	

	// we are no longer in multi-threaded mode
	stackBase = nullptr;

	// kill the RAM used to start all of the threads
	free (origStackBase);

	// and make it so that we have no thread pinned pages
	threadPinnedPages.resize (1);
}

pthread_mutex_t *MyDB_BufferManager :: getLock () {
	return &myLock;
}

bool MyDB_BufferManager :: checkCannotExpell (void *checkMe) {
	for (void *temp : threadPinnedPages) {
		if (checkMe == temp)
			return true;
	}
	return false;
}

bool MyDB_BufferManager :: checkIfThreadPinned (void *checkMe) {

	// this serves to gives us the location of our call stack
        int i;

	// if we are not in one of the created worker threads, then we put this at the end of the list
	if (stackBase == nullptr || !(((char *) &i) >= (char *) stackBase && ((char *) &i) < (char *) stackEnd)) {
		return threadPinnedPages[0] == checkMe;
	}

	// we are one of the worker threads... so see how many size 2^22 segments we are from the base of the call stack
	size_t temp = (size_t) ((char *) &i - (char *) stackBase);
	return threadPinnedPages[1 + (temp >> 22)] == checkMe;
}
	
void MyDB_BufferManager :: setCannotExpell (void *setMe) {

	// this serves to gives us the location of our call stack
        int i;

	// if we are not in one of the created worker threads, then we put this at the end of the list
	if (stackBase == nullptr || !(((char *) &i) >= (char *) stackBase && ((char *) &i) < (char *) stackEnd)) {
		threadPinnedPages[0] = setMe;
		return;
	}

	// we are one of the worker threads... so see how many size 2^22 segments we are from the base of the call stack
	size_t temp = (size_t) ((char *) &i - (char *) stackBase);
	threadPinnedPages[1 + (temp >> 22)] = setMe;
}

void MyDB_BufferManager :: kickOutPage () {
	
	if (lastUsed.size () == 0)
		cout << "Bad: all buffer memory is exhausted!";

	// find the oldest page
	auto it = lastUsed.begin();

	// and now find the oldest page that can be expelled
	while (true) {
		auto &page = *it;
		if (!checkCannotExpell (page->bytes))
			break;
		it++;
	}
	auto page = *it;

	// write it back if necessary
	if (page->isDirty) {
		lseek (fds[page->myTable], page->pos * pageSize, SEEK_SET);
		write (fds[page->myTable], page->bytes, pageSize);
		page->isDirty = false;
	}

	// remove it
	lastUsed.erase (page);

	// remember its RAM
	availableRam.push_back (page->bytes);
	page->bytes = nullptr;

	// if this guy has no references, kill him
	if (page->refCount == 0)
		killPage (page);
}

void MyDB_BufferManager :: killPage (MyDB_PagePtr killMe) {

	// if this is an anon page...
	if (killMe->myTable == nullptr) {

		// recycle him
		availablePositions.push (killMe->pos);
		if (killMe->bytes != nullptr) {
			availableRam.push_back (killMe->bytes);
		}

		// if he is in the LRU list, remove him
		if (lastUsed.count (killMe) != 0) {
			auto page = *(lastUsed.find (killMe));
			lastUsed.erase (page);
		}

	// if this is a pinned, non-anon page whose data is buffered it converts...
	} else if (lastUsed.count (killMe) == 0 && killMe->bytes != nullptr) {
		killMe->timeTick = ++lastTimeTick;
		lastUsed.insert (killMe);

	// this guy has no data, so just kill him
	} else if (killMe->bytes == nullptr) {
		pair <MyDB_TablePtr, long> whichPage = make_pair (killMe->myTable, killMe->pos);
		allPages.erase (whichPage);
	}
}

// idea: when I access a page, I check to make sure that it is the same page as last time
void MyDB_BufferManager :: access (MyDB_PagePtr updateMe) {
	
	// if this page was just accessed, get outta here
	if (updateMe->timeTick > lastTimeTick - (numPages / 2) && updateMe->bytes != nullptr) {

		// if this thread has already pinned this page by access it, then we are good
		if (checkIfThreadPinned (updateMe->bytes)) {
			return;

		// otherwise, we mark this page as thread pinned
		} else {
			Lock temp (getLock ());
			if (updateMe->bytes != nullptr) {
				setCannotExpell (updateMe->bytes);
				return;
			}
		}
	}

	{
		Lock temp (getLock ());

		// first, see if it is currently in the LRU list; if it is, update it
		if (lastUsed.count (updateMe) == 1) {

			// update the LRU value
			auto page = *(lastUsed.find (updateMe));
			lastUsed.erase (page);
			updateMe->timeTick = ++lastTimeTick;
			lastUsed.insert (updateMe);

			// and mark this page as thread pinned
			setCannotExpell (updateMe->bytes);

			return;
		} 
	}

	// the file descriptor to read from
	int fd;

	{
		Lock temp (getLock ());

		// not in the LRU list means that we don't have its contents buffered
		// see if there is space
		if (availableRam.size () == 0)
			kickOutPage ();

		// if there is no space, we cannot do anything
		if (availableRam.size () == 0) {
			cout << "Can't get any RAM to read a page!!\n";
			exit (1);
		}

		// get some RAM for the page
		updateMe->bytes = availableRam[availableRam.size () - 1]; 
		setCannotExpell (updateMe->bytes);
		updateMe->numBytes = pageSize;
		availableRam.pop_back ();

		// note that the page is now thread pinned
		setCannotExpell (updateMe->bytes);

		// change the LRU info
		updateMe->timeTick = ++lastTimeTick;
		lastUsed.insert (updateMe);

		fd = fds[updateMe->myTable];
	}

	// and read it
	lseek (fd, updateMe->pos * pageSize, SEEK_SET);
	read (fd, updateMe->bytes, pageSize);
}

MyDB_PageHandle MyDB_BufferManager :: getPinnedPage (MyDB_TablePtr whichTable, long i) {

	int fdToRead = -1;	
	MyDB_PagePtr returnVal;

	{
		Lock temp (getLock ());

		// open the file, if it is not open
		if (fds.count (whichTable) == 0) {
			int fd = open (whichTable->getStorageLoc ().c_str (), O_CREAT | O_RDWR, 0666);
			fds[whichTable] = fd;
		}
	
		// make sure we don't have a null table
		if (whichTable == nullptr) {
			cout << "Can't allocate a page with a null table!!\n";
			exit (1);
		}

		// first, see if the page is there in the buffer
		pair <MyDB_TablePtr, long> whichPage = make_pair (whichTable, i);

		// see if we already know him
		if (allPages.count (whichPage) == 0) {

			// in this case, we do not
			returnVal = make_shared <MyDB_Page> (whichTable, i, *this);
			allPages [whichPage] = returnVal;

		// in this case, we do
		} else {
	
			// get him out of the LRU list if he is there
			returnVal = allPages [whichPage];
			if (lastUsed.count (returnVal) != 0) {
				auto page = *(lastUsed.find (returnVal));
		       		lastUsed.erase (page);
			}
		}

		// see if we need to get his data
		if (returnVal->bytes == nullptr) {

			// see if there is space to make a pinned page
			if (availableRam.size () == 0)
				kickOutPage ();
	
			// if there is no space, we cannot do anything
			if (availableRam.size () == 0) 
				return nullptr;
	
			// set up the return val
			returnVal->bytes = availableRam[availableRam.size () - 1];
			returnVal->numBytes = pageSize;
			availableRam.pop_back ();
			fdToRead = fds[returnVal->myTable];
		}

	}

	if (fdToRead != -1) {
		lseek (fdToRead, returnVal->pos * pageSize, SEEK_SET);
		read (fdToRead, returnVal->bytes, pageSize);
	}

	// get outta here
	return make_shared <MyDB_PageHandleBase> (returnVal);
}

MyDB_PageHandle MyDB_BufferManager :: getPinnedPage () {

	// get a page to return
	MyDB_PageHandle returnVal = getPage ();

	Lock temp (getLock ());

	// see if there is space to make a pinned page
	if (availableRam.size () == 0)
		kickOutPage ();

	// if there is no space, we cannot do anything
	if (availableRam.size () == 0) 
		return nullptr;

	returnVal->page->bytes = availableRam[availableRam.size () - 1];
	setCannotExpell (returnVal->page->bytes);
	returnVal->page->numBytes = pageSize;
	availableRam.pop_back ();

	// and get outta here
	return returnVal;
}

void MyDB_BufferManager :: unpin (MyDB_PagePtr unpinMe) {

	Lock temp (getLock ());
	unpinMe->timeTick = ++lastTimeTick;
	lastUsed.insert (unpinMe);
}

MyDB_BufferManager :: MyDB_BufferManager (size_t pageSizeIn, size_t numPagesIn, string tempFileIn) {

	// remember the inputs
	pageSize = pageSizeIn;

	// this is the location where we write temp pages
	tempFile = tempFileIn;

	// there is one thead pinned page... having to do with the main thread
	threadPinnedPages.resize (1);
	threadPinnedPages [0] = nullptr;

	// we are not running in multi-threaded mode
	stackBase = nullptr;

	// start at time tick zero
	lastTimeTick = 0;

	// initialize the mutex
	pthread_mutex_init (&myLock, nullptr);

	// position in temp file
	lastTempPos = 0;

	// the number of pages; we add some extra pages just to be safe
	numPages = numPagesIn + 10;

	// create all of the RAM
	for (size_t i = 0; i < numPages; i++) {
		availableRam.push_back (malloc (pageSizeIn));
	}	
}

MyDB_BufferManager :: ~MyDB_BufferManager () {
	
	if (threadPinnedPages.size () != 1) {
		std :: cout << "This is bad.  It appears the buffer manager is being killed with some threads outstanding.\n";
	}

	for (auto page : allPages) {

		if (page.second->bytes != nullptr) {

			// write it back if necessary
			if (page.second->isDirty) {
				lseek (fds[page.second->myTable], page.second->pos * pageSize, SEEK_SET);
				write (fds[page.second->myTable], page.second->bytes, pageSize);
			}

			free (page.second->bytes);
			page.second->bytes = nullptr;
		}
	}

	// delete the rest of the RAM
	for (auto ram : availableRam) {
		free (ram);
	}

	// get rid of the lock
	pthread_mutex_destroy (&myLock);
	
	// finally, close the files
	for (auto fd : fds) {
		close (fd.second);
	}

	unlink (tempFile.c_str ());
}


#endif


