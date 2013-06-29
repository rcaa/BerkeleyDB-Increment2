package com.deletedb;

import java.util.Iterator;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.txn.Txn;

public privileged aspect TxnDeleteSupportAbstract {

	pointcut hook_commitDeletedDatabaseState(Txn txn) : call(void Txn.hook_commitDeletedDatabaseState()) && this(txn);

	pointcut hook_abortDeletedDatabaseState(Txn txn) : call(void Txn.hook_abortDeletedDatabaseState()) && this(txn);

	pointcut close(Txn txn) : call(void Txn.close(boolean)) && withincode(long Txn.commit(..)) && this(txn);

	pointcut hook_cleanupDatabaseImpls(Txn txn) : call(void Txn.hook_cleanupDatabaseImpls()) && withincode(* Txn.abortInternal(..)) && this(txn);
	
	after(Txn txn) throws DatabaseException:
		hook_commitDeletedDatabaseState(txn){
		/*
		 * Set database state for deletes before releasing any write locks.
		 */
		txn.setDeletedDatabaseState(true);
	}

	after(Txn txn) throws DatabaseException:
		hook_abortDeletedDatabaseState(txn){
		/*
		 * Set database state for deletes before releasing any write locks.
		 */
		txn.setDeletedDatabaseState(false);
	}

	before(Txn txn) throws DatabaseException:
		close(txn){
		/*
		 * Purge any databaseImpls not needed as a result of the commit. Be sure
		 * to do this outside the synchronization block, to avoid conflict
		 * w/checkpointer.
		 */
		txn.cleanupDatabaseImpls(true);
	}

	before(Txn txn) throws DatabaseException:
		hook_cleanupDatabaseImpls(txn){
		/*
		 * Purge any databaseImpls not needed as a result of the abort. Be sure
		 * to do this outside the synchronization block, to avoid conflict
		 * w/checkpointer.
		 */
		txn.cleanupDatabaseImpls(false);
	}
	
	/*
	 * Leftover databaseImpls that are a by-product of database operations like
	 * removeDatabase(), truncateDatabase() will be deleted after the write
	 * locks are released. However, do set the database state appropriately
	 * before the locks are released.
	 */
	private void Txn.setDeletedDatabaseState(boolean isCommit)
			throws DatabaseException {

		if (deletedDatabases != null) {
			Iterator iter = deletedDatabases.iterator();
			while (iter.hasNext()) {
				DatabaseCleanupInfo info = (DatabaseCleanupInfo) iter.next();
				if (info.deleteAtCommit == isCommit) {
					info.dbImpl.startDeleteProcessing();
				}
			}
		}
	}

	/**
	 * Cleanup leftover databaseImpls that are a by-product of database
	 * operations like removeDatabase(), truncateDatabase().
	 * 
	 * This method must be called outside the synchronization on this txn,
	 * because it calls deleteAndReleaseINs, which gets the TxnManager's allTxns
	 * latch. The checkpointer also gets the allTxns latch, and within that
	 * latch, needs to synchronize on individual txns, so we must avoid a
	 * latching hiearchy conflict.
	 */
	private void Txn.cleanupDatabaseImpls(boolean isCommit)
			throws DatabaseException {

		if (deletedDatabases != null) {
			/* Make a copy of the deleted databases while synchronized. */
			DatabaseCleanupInfo[] infoArray;
			synchronized (this) {
				infoArray = new DatabaseCleanupInfo[deletedDatabases.size()];
				deletedDatabases.toArray(infoArray);
			}
			for (int i = 0; i < infoArray.length; i += 1) {
				DatabaseCleanupInfo info = infoArray[i];
				if (info.deleteAtCommit == isCommit) {
					info.dbImpl.releaseDeletedINs();
				}
			}
			deletedDatabases = null;
		}
	}
}
