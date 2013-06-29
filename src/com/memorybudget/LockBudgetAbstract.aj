package com.memorybudget;

import com.sleepycat.je.txn.Lock;
import com.sleepycat.je.txn.LockInfo;
import com.sleepycat.je.txn.LockManager;
import com.sleepycat.je.txn.Locker;
import java.util.Collections;
import java.util.Set;

public privileged aspect LockBudgetAbstract {

	pointcut addWaiterOrOwner(int lockTableIndex, Lock lock) : (execution(void Lock.addWaiterToEndOfList(LockInfo, int)) || 
			execution(void Lock.addWaiterToHeadOfList(LockInfo, int)) ||
			execution(void Lock.addOwner(LockInfo, int))) 
			&& args(LockInfo,lockTableIndex) && this(lock);

	pointcut flushOwner(int lockTableIndex, Lock lock) : (execution(boolean Lock.flushWaiter(Locker, int)) ||
			execution(boolean Lock.flushOwner(LockInfo, int))) 
			&& args(*,lockTableIndex) && this(lock);

	pointcut flushOwner2(int lockTableIndex, Lock lock) : execution(LockInfo Lock.flushOwner(Locker,int)) &&
	args(Locker,lockTableIndex) && this(lock);

	pointcut release(int lockTableIndex, Lock lock) 
	: execution(Set Lock.release(Locker,int)) &&
	args(Locker,lockTableIndex) && this(lock);

	pointcut hook_removedLockInfos(int numRemovedLockInfos, int lockTableIndex,
			Lock lock) 
	: execution(void Lock.hook_removedLockInfos(int,int)) 
	&& args(numRemovedLockInfos,lockTableIndex) && this(lock);

	pointcut constructorLock(LockManager lockManager) : call(Lock.new(Long)) && this(lockManager);
	
	after(int lockTableIndex, Lock lock):
		addWaiterOrOwner(lockTableIndex, lock){
		lock.memoryBudget.updateLockMemoryUsage(MemoryBudget.LOCKINFO_OVERHEAD,
				lockTableIndex);// MB

	}

	// made homogenic myself by chaning return values
	after(int lockTableIndex, Lock lock)returning (boolean removed):
		flushOwner(lockTableIndex, lock) {
		if (removed) {
			lock.memoryBudget.updateLockMemoryUsage(REMOVE_LOCKINFO_OVERHEAD,
					lockTableIndex);// MB
		}
	}

	// made homogenic myself by chaning return values
	after(int lockTableIndex, Lock lock) returning (LockInfo flushedInfo) :
		flushOwner2(lockTableIndex, lock) {
		if (flushedInfo != null) {
			lock.memoryBudget.updateLockMemoryUsage(REMOVE_LOCKINFO_OVERHEAD,
					lockTableIndex);// MB
		}
	}

	after(int lockTableIndex, Lock lock)returning (Set result):
		release(lockTableIndex, lock){

		if (result != Collections.EMPTY_SET) {
			lock.memoryBudget.updateLockMemoryUsage(result.size()
					* REMOVE_LOCKINFO_OVERHEAD, lockTableIndex);// MB
		}
	}

	before(int numRemovedLockInfos, int lockTableIndex, Lock lock):
		hook_removedLockInfos(numRemovedLockInfos,lockTableIndex, lock) {
		lock.memoryBudget.updateLockMemoryUsage(
				0 - (numRemovedLockInfos * MemoryBudget.LOCKINFO_OVERHEAD),
				lockTableIndex);// MB
	}

	after(LockManager lockManager) returning (Lock newLock) :
		constructorLock(lockManager){

		newLock.setMemoryBudget(lockManager.memoryBudget);
	}
	
	private static final int REMOVE_LOCKINFO_OVERHEAD = 0 - MemoryBudget.LOCKINFO_OVERHEAD;// MB

	private MemoryBudget Lock.memoryBudget = null;

	void Lock.setMemoryBudget(MemoryBudget memoryBudget) {
		this.memoryBudget = memoryBudget;
	}
	
	declare error:call(Lock.new(..)) && !within(LockManager):"cannot instanciate Lock class outside LockManager (needs setMemoryManager() call)";
}