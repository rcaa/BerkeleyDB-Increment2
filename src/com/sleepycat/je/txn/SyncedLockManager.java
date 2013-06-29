/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002-2006
 *      Sleepycat Software.  All rights reserved.
 *
 * $Id: SyncedLockManager.java,v 1.1.6.1.2.4 2006/10/25 17:21:17 ckaestne Exp $
 */

package com.sleepycat.je.txn;

import java.util.Set;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;

/**
 * SyncedLockManager uses the synchronized keyword to implement its critical
 * sections.
 */
public class SyncedLockManager extends LockManager {

	public SyncedLockManager(EnvironmentImpl envImpl) throws DatabaseException {

		super(envImpl);
	}
	//Lck replaced all syncs to lockTableLatches with lockTables

	/**
	 * @see LockManager#attemptLock
	 */
	protected LockAttemptResult attemptLock(Long nodeId, Locker locker,
			LockType type, boolean nonBlockingRequest) throws DatabaseException {

		int lockTableIndex = getLockTableIndex(nodeId);
		synchronized (lockTables[lockTableIndex]) {
			return attemptLockInternal(nodeId, locker, type,
					nonBlockingRequest, lockTableIndex);
		}
	}

	/**
	 * @see LockManager#makeTimeoutMsg
	 */
	protected String makeTimeoutMsg(String lockOrTxn, Locker locker,
			long nodeId, LockType type, LockGrantType grantType, Lock useLock,
			long timeout, long start, long now, DatabaseImpl database) {

		int lockTableIndex = getLockTableIndex(nodeId);
		synchronized (lockTables[lockTableIndex]) {
			return makeTimeoutMsgInternal(lockOrTxn, locker, nodeId, type,
					grantType, useLock, timeout, start, now, database);
		}
	}

	/**
	 * @see LockManager#releaseAndNotifyTargets
	 */
	protected Set releaseAndFindNotifyTargets(long nodeId, Lock lock,
			Locker locker, boolean removeFromLocker) throws DatabaseException {

		long nid = nodeId;
		if (nid == -1) {
			nid = lock.getNodeId().longValue();
		}
		int lockTableIndex = getLockTableIndex(nid);
		synchronized (lockTables[lockTableIndex]) {
			return releaseAndFindNotifyTargetsInternal(nodeId, lock, locker,
					removeFromLocker, lockTableIndex);
		}
	}

	/**
	 * @see LockManager#transfer
	 */
	protected void transfer(long nodeId, Locker owningLocker, Locker destLocker,
			boolean demoteToRead) throws DatabaseException {

		int lockTableIndex = getLockTableIndex(nodeId);
		synchronized (lockTables[lockTableIndex]) {
			transferInternal(nodeId, owningLocker, destLocker, demoteToRead,
					lockTableIndex);
		}
	}

	/**
	 * @see LockManager#transferMultiple
	 */
	protected void transferMultiple(long nodeId, Locker owningLocker, Locker[] destLockers)
			throws DatabaseException {

		int lockTableIndex = getLockTableIndex(nodeId);
		synchronized (lockTables[lockTableIndex]) {
			transferMultipleInternal(nodeId, owningLocker, destLockers,
					lockTableIndex);
		}
	}

	/**
	 * @see LockManager#demote
	 */
	protected void demote(long nodeId, Locker locker) throws DatabaseException {

		int lockTableIndex = getLockTableIndex(nodeId);
		synchronized (lockTables[lockTableIndex]) {
			demoteInternal(nodeId, locker, lockTableIndex);
		}
	}

	/**
	 * @see LockManager#isLocked
	 */
	protected boolean isLocked(Long nodeId) {

		int lockTableIndex = getLockTableIndex(nodeId);
		synchronized (lockTables[lockTableIndex]) {
			return isLockedInternal(nodeId, lockTableIndex);
		}
	}

	/**
	 * @see LockManager#isOwner
	 */
	protected boolean isOwner(Long nodeId, Locker locker, LockType type) {

		int lockTableIndex = getLockTableIndex(nodeId);
		synchronized (lockTables[lockTableIndex]) {
			return isOwnerInternal(nodeId, locker, type, lockTableIndex);
		}
	}

	/**
	 * @see LockManager#isWaiter
	 */
	protected boolean isWaiter(Long nodeId, Locker locker) {

		int lockTableIndex = getLockTableIndex(nodeId);
		synchronized (lockTables[lockTableIndex]) {
			return isWaiterInternal(nodeId, locker, lockTableIndex);
		}
	}

	/**
	 * @see LockManager#nWaiters
	 */
	protected int nWaiters(Long nodeId) {

		int lockTableIndex = getLockTableIndex(nodeId);
		synchronized (lockTables[lockTableIndex]) {
			return nWaitersInternal(nodeId, lockTableIndex);
		}
	}

	/**
	 * @see LockManager#nOwners
	 */
	protected int nOwners(Long nodeId) {

		int lockTableIndex = getLockTableIndex(nodeId);
		synchronized (lockTables[lockTableIndex]) {
			return nOwnersInternal(nodeId, lockTableIndex);
		}
	}

	/**
	 * @see LockManager#getWriterOwnerLocker
	 */
	Locker getWriteOwnerLocker(Long nodeId) throws DatabaseException {

		int lockTableIndex = getLockTableIndex(nodeId);
		synchronized (lockTables[lockTableIndex]) {
			return getWriteOwnerLockerInternal(nodeId, lockTableIndex);
		}
	}

	/**
	 * @see LockManager#validateOwnership
	 */
	protected boolean validateOwnership(Long nodeId, Locker locker,
			LockType type, boolean flushFromWaiters/*//M.B , LogBufferBudget mb*/)
			throws DatabaseException {

		int lockTableIndex = getLockTableIndex(nodeId);
		synchronized (lockTables[lockTableIndex]) {
			return validateOwnershipInternal(nodeId, locker, type,
					flushFromWaiters, /*//M.B mb,*/ lockTableIndex);
		}
	}


}
