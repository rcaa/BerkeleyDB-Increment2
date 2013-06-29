package com.memorybudget;

import com.sleepycat.je.txn.LockManager;
import com.sleepycat.je.txn.LockManager.LockTable;
import com.sleepycat.je.dbi.EnvironmentImpl;
import java.util.Map;

public privileged aspect LockManagerBudgetAbstract {

	pointcut setLockManager(LockManager lm) : set(EnvironmentImpl LockManager.envImpl) && target(lm);

	pointcut putObject(LockManager lm, LockTable lockTable) 
	: call(Object Map.put(Object, Object)) && target(lockTable) && this(lm);

	pointcut removeObject(LockManager lm, LockTable lockTable) 
	: call(Object Map.remove(Object)) && target(lockTable) && this(lm);

	after(LockManager lm) : setLockManager(lm){
		lm.memoryBudget = lm.envImpl.getMemoryBudget();// MB
	}

	after(LockManager lm, LockTable lockTable) : putObject(lm, lockTable) {
		lm.memoryBudget.updateLockMemoryUsage(TOTAL_LOCK_OVERHEAD,
				lockTable.lockTableIndex);// MB
	}

	after(LockManager lm, LockTable lockTable):
		removeObject(lm, lockTable) {
		lm.memoryBudget.updateLockMemoryUsage(REMOVE_TOTAL_LOCK_OVERHEAD,
				lockTable.lockTableIndex);// MB
	}

	static final long TOTAL_LOCK_OVERHEAD = MemoryBudget.LOCK_OVERHEAD// MB
			+ MemoryBudget.HASHMAP_ENTRY_OVERHEAD + MemoryBudget.LONG_OVERHEAD;

	private static final long REMOVE_TOTAL_LOCK_OVERHEAD = 0 - TOTAL_LOCK_OVERHEAD;

	private MemoryBudget LockManager.memoryBudget;// MB
}
