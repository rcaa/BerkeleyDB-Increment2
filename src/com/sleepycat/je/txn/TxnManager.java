/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002-2006
 *      Sleepycat Software.  All rights reserved.
 *
 * $Id: TxnManager.java,v 1.1.6.3.2.6 2007/02/27 17:45:14 ckaestne Exp $
 */

package com.sleepycat.je.txn;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.transaction.xa.Xid;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.LogBufferBudget;
import com.sleepycat.je.utilint.DbLsn;

/**
 * Class to manage transactions. Basically a Set of all transactions with add
 * and remove methods and a latch around the set.
 */
public class TxnManager {

	/*
	 * All NullTxns share the same id so as not to eat from the id number space.
	 */
	static final long NULL_TXN_ID = -1;

	//Lck made public
	private static final String DEBUG_NAME = TxnManager.class.getName();

	private LockManager lockManager;

	private EnvironmentImpl env;


	private Set allTxns;

	/* Maps Xids to Txns. */
	private Map allXATxns;

	/* Maps Threads to Txns when there are thread implied transactions. */
	private Map thread2Txn;

	private long lastUsedTxnId;

	private int nActiveSerializable;



	public TxnManager(EnvironmentImpl env) throws DatabaseException {

//		Lck if (EnvironmentImpl.getFairLatches()) {
//			lockManager = new LatchedLockManager(env);
//		} else {
			if (env.isNoLocking()) {
				lockManager = new DummyLockManager(env);
			} else {
				lockManager = new SyncedLockManager(env);
			}
//		}

		this.env = env;
		allTxns = new HashSet();
		allXATxns = Collections.synchronizedMap(new HashMap());
		thread2Txn = Collections.synchronizedMap(new HashMap());


		lastUsedTxnId = 0;
	}

	/**
	 * Set the txn id sequence.
	 */
	synchronized public void setLastTxnId(long lastId) {
		this.lastUsedTxnId = lastId;
	}

	/**
	 * Get the last used id, for checkpoint info.
	 */
	public synchronized long getLastTxnId() {
		return lastUsedTxnId;
	}

	/**
	 * Get the next transaction id to use.
	 */
	synchronized long incTxnId() {
		return ++lastUsedTxnId;
	}



	/**
	 * Give transactions and environment access to lock manager.
	 */
	public LockManager getLockManager() {
		return lockManager;
	}

	/**
	 * Called when txn is created.
	 */
	void registerTxn(Txn txn) throws DatabaseException {

		allTxns.add(txn);
		if (txn.isSerializableIsolation()) {
			nActiveSerializable++;
		}
	}

	/**
	 * Called when txn ends.
	 */
	void unRegisterTxn(Txn txn, boolean isCommit) throws DatabaseException {

			allTxns.remove(txn);
			/* Remove any accumulated MemoryBudget delta for the Txn. */
//			env.getMemoryBudget().updateMiscMemoryUsage(
//					txn.getAccumulatedDelta() - txn.getInMemorySize());

			if (txn.isSerializableIsolation()) {
				nActiveSerializable--;
			}
	}

	/**
	 * Called when txn is created.
	 */
	public void registerXATxn(Xid xid, Txn txn, boolean isPrepare)
			throws DatabaseException {

		if (!allXATxns.containsKey(xid)) {
			allXATxns.put(xid, txn);
		}


	}

	/**
	 * Called when txn ends.
	 */
	void unRegisterXATxn(Xid xid, boolean isCommit) throws DatabaseException {

		if (allXATxns.remove(xid) == null) {
			throw new DatabaseException("XA Transaction " + xid
					+ " can not be unregistered.");
		}

	}

	/**
	 * Retrieve a Txn object from an Xid.
	 */
	public Txn getTxnFromXid(Xid xid) throws DatabaseException {

		return (Txn) allXATxns.get(xid);
	}


	public Xid[] XARecover() throws DatabaseException {

		Set xidSet = allXATxns.keySet();
		Xid[] ret = new Xid[xidSet.size()];
		ret = (Xid[]) xidSet.toArray(ret);

		return ret;
	}

	/**
	 * Returns whether there are any active serializable transactions, excluding
	 * the transaction given (if non-null). This is intentionally returned
	 * without latching, since latching would not make the act of reading an
	 * integer more atomic than it already is.
	 */
	public boolean areOtherSerializableTransactionsActive(Locker excludeLocker) {
		int exclude = (excludeLocker != null && excludeLocker
				.isSerializableIsolation()) ? 1 : 0;
		return (nActiveSerializable - exclude > 0);
	}

	/**
	 * Get the earliest LSN of all the active transactions, for checkpoint.
	 */
	public long getFirstActiveLsn() throws DatabaseException {

		/*
		 * Note that the latching hierarchy calls for getting allTxnLatch first,
		 * then synchronizing on individual txns.
		 */
		long firstActive = DbLsn.NULL_LSN;
			Iterator iter = allTxns.iterator();
			while (iter.hasNext()) {
				long txnFirstActive = ((Txn) iter.next()).getFirstActiveLsn();
				if (firstActive == DbLsn.NULL_LSN) {
					firstActive = txnFirstActive;
				} else if (txnFirstActive != DbLsn.NULL_LSN) {
					if (DbLsn.compareTo(txnFirstActive, firstActive) < 0) {
						firstActive = txnFirstActive;
					}
				}
			}
		return firstActive;
	}

	/*
	 * Statistics
	 */

	

}
