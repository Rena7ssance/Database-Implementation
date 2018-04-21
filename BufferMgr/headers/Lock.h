
#include <pthread.h>

#ifndef LOCK_H
#define LOCK_H

// this class encapsulates a lock on a pthread mutex.  The unlock is implicit;
// as soon as the object goes out of scope, the underlying pthread mutex is
// released.
class Lock {

public:

	Lock () {
		myLock = nullptr;
	}

	~Lock () {
	 	if (myLock != nullptr) {
			pthread_mutex_unlock (myLock);
		}
	}

	Lock (pthread_mutex_t *inLock) {
		myLock = inLock;
		pthread_mutex_lock (myLock);
	}

private:

	pthread_mutex_t *myLock;
};

#endif
