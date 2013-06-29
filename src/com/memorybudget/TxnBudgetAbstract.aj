package com.memorybudget;

import com.sleepycat.je.txn.Txn;
import com.sleepycat.je.txn.TxnManager;
import java.util.Map;

public privileged aspect TxnBudgetAbstract {

	pointcut putMap(TxnManager lm) : call(Object Map.put(Object, Object)) && this(lm);

	pointcut txnManagerRemove(TxnManager lm) : call(Object *.remove(Object)) && this(lm);

	pointcut registerTxn(Txn txn) : execution(void TxnManager.registerTxn(Txn)) && args(txn);

	pointcut addObject(Txn txn) :  call(boolean *.add(Object)) && target(Txn.ReadLocksSet) && this(txn);

	pointcut txnRemoveObejct(Txn txn) : call(boolean *.remove(Object)) && target(Txn.ReadLocksSet) && this(txn);

	pointcut readLocksSetConstructor(Txn txn) : call(Txn.ReadLocksSet.new()) && this(txn);

	pointcut txnPutObejct(Txn txn) : call(Object *.put(Object, Object)) && target(Txn.WriteInfoMap) && this(txn);

	pointcut txnRemoveObject2(Txn txn) : call(Object *.remove(Object)) && target(Txn.WriteInfoMap) && this(txn);

	pointcut writeInfoMapConstructor(Txn txn) : call(Txn.WriteInfoMap.new()) && this(txn);
	
	after(TxnManager lm) : putMap(lm) {
		lm.env.getMemoryBudget().updateMiscMemoryUsage(
				MemoryBudget.HASHMAP_ENTRY_OVERHEAD);
	}

	after(TxnManager lm) : txnManagerRemove(lm) {
		lm.env.getMemoryBudget().updateMiscMemoryUsage(
				0 - MemoryBudget.HASHMAP_ENTRY_OVERHEAD);
	}

	before(Txn txn) : registerTxn(txn) {

		/*
		 * Note: readLocks, writeInfo, undoDatabases, deleteDatabases are
		 * initialized lazily in order to conserve memory. WriteInfo and
		 * undoDatabases are treated as a package deal, because they are both
		 * only needed if a transaction does writes.
		 * 
		 * When a lock is added to this transaction, we add the collection entry
		 * overhead to the memory cost, but don't add the lock itself. That's
		 * taken care of by the Lock class.
		 */
		txn.updateMemoryUsage(MemoryBudget.TXN_OVERHEAD);// MB

	}

	after(Txn txn) : addObject(txn) {
		txn.updateMemoryUsage(READ_LOCK_OVERHEAD);// MB
	}

	after(Txn txn) : txnRemoveObejct(txn) {
		txn.updateMemoryUsage(0 - READ_LOCK_OVERHEAD);// MB
	}

	after(Txn txn) : readLocksSetConstructor(txn) {
		txn.updateMemoryUsage(MemoryBudget.HASHMAP_OVERHEAD);// MB
	}

	// TODO dynamic tests, advise more potential calls than necessary. this is
	// not homogenic!
	// this is even hard to figure out manually where to match.
	after(Txn txn) : txnPutObejct(txn) {

		txn.updateMemoryUsage(WRITE_LOCK_OVERHEAD);// MB
	}

	after(Txn txn) : txnRemoveObject2(txn) {
		txn.updateMemoryUsage(0 - WRITE_LOCK_OVERHEAD);// MB
	}

	after(Txn txn) : writeInfoMapConstructor(txn) {
		txn.updateMemoryUsage(MemoryBudget.TWOHASHMAPS_OVERHEAD);// MB
	}
	
	private final int READ_LOCK_OVERHEAD = MemoryBudget.HASHSET_ENTRY_OVERHEAD;

	private final int WRITE_LOCK_OVERHEAD = MemoryBudget.HASHMAP_ENTRY_OVERHEAD
			+ MemoryBudget.LONG_OVERHEAD;// MB
	
	private int Txn.inMemorySize;// MB

	/*
	 * accumluted memory budget delta. Once this exceeds ACCUMULATED_LIMIT we
	 * inform the MemoryBudget that a change has occurred.
	 */
	private int Txn.accumulatedDelta = 0;

	/*
	 * Max allowable accumulation of memory budget changes before MemoryBudget
	 * should be updated. This allows for consolidating multiple calls to
	 * updateXXXMemoryBudget() into one call. Not declared final so that unit
	 * tests can modify this. See SR 12273.
	 */
	public static int Txn.ACCUMULATED_LIMIT = 10000;
	
	private void Txn.updateMemoryUsage(int delta) {
		inMemorySize += delta;
		accumulatedDelta += delta;
		if (accumulatedDelta > Txn.ACCUMULATED_LIMIT
				|| accumulatedDelta < -Txn.ACCUMULATED_LIMIT) {
			envImpl.getMemoryBudget().updateMiscMemoryUsage(accumulatedDelta);
			accumulatedDelta = 0;
		}
	}
	
	int Txn.getAccumulatedDelta() {
		return accumulatedDelta;
	}

	int Txn.getInMemorySize() {// MB
		return inMemorySize;
	}
}
