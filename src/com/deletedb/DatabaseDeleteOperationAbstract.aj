package com.deletedb;

import java.util.HashSet;
import java.util.Set;
import java.util.SortedMap;
import com.memorybudget.MemoryBudget;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DatabaseUtil;
import com.sleepycat.je.Environment;
import com.sleepycat.je.OperationContext;
import com.sleepycat.je.cleaner.Cleaner;
import com.sleepycat.je.cleaner.UtilizationTracker;
import com.sleepycat.je.dbi.CursorImpl;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.SortedLSNTreeWalker;
import com.sleepycat.je.dbi.SortedLSNTreeWalker.TreeNodeProcessor;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.recovery.Checkpointer;
import com.sleepycat.je.recovery.Checkpointer.CheckpointReference;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.txn.Locker;
import com.sleepycat.je.txn.LockerFactory;
import com.sleepycat.je.txn.Txn;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.txn.BasicLocker;
import com.sleepycat.je.txn.BuddyLocker;
import com.evictor.Evictor;
import com.incompressor.INCompressor;
import com.sleepycat.je.Database.DbState;
import com.sleepycat.je.cleaner.UtilizationProfile;

public privileged aspect DatabaseDeleteOperationAbstract {

	pointcut databaseConstructor(DatabaseImpl db) : execution(DatabaseImpl.new(..)) && this(db);

	pointcut processPending(Cleaner cleaner) : execution(void Cleaner.processPending()) && this(cleaner);

	pointcut hook_checkDeletedDb(DatabaseImpl db, Cleaner cleaner) : execution(boolean Cleaner.hook_checkDeletedDb(DatabaseImpl)) && args(db) && target(cleaner);

	pointcut hook_isDbGone(DatabaseImpl db) : execution(boolean UtilizationProfile.hook_isDbGone(DatabaseImpl)) && args(db);

	pointcut hook_checkDeleted(CheckpointReference targetRef,
			SortedMap dirtyMap, boolean allowDeltas, long checkpointStart,
			Checkpointer checkpointer) 
	: execution(void Checkpointer.hook_checkDeleted(CheckpointReference, SortedMap, boolean, long))
		&& args(targetRef, dirtyMap, allowDeltas, checkpointStart) && this(checkpointer);

	pointcut checkRequiredDbState(Database db) : call(void checkRequiredDbState(DbState, String)) && 
		withincode(* Database.preload(..)) && this(db);

	pointcut hook_getDbExistsEnvironment(DatabaseImpl database) : execution(boolean Environment.hook_getDbExists(DatabaseImpl)) && args(database);

	pointcut hook_checkDeletedEvictor(DatabaseImpl db, IN in) : execution(boolean Evictor.hook_checkDeleted(DatabaseImpl, IN)) && args(db, in);

	pointcut hook_getDbExistsINCompressor(DatabaseImpl database) : execution(boolean INCompressor.hook_getDbExists(DatabaseImpl)) && args(database);

	after(DatabaseImpl db) : databaseConstructor(db){
		db.deleteState = NOT_DELETED;
	}

	after(Cleaner cleaner) throws DatabaseException :
		processPending(cleaner){
		DbTree dbMapTree = cleaner.env.getDbMapTree();
		DatabaseId[] pendingDBs = cleaner.fileSelector.getPendingDBs();
		if (pendingDBs != null) {
			for (int i = 0; i < pendingDBs.length; i += 1) {
				DatabaseId dbId = pendingDBs[i];
				DatabaseImpl db = dbMapTree.getDb(dbId, cleaner.lockTimeout);
				if (db == null || db.isDeleteFinished()) {
					cleaner.fileSelector.removePendingDB(dbId);
				}
			}
		}
	}

	// TODO could have been a great homogeneous pointcut but not possible due to
	// local variables used for the logging feature
	boolean around(DatabaseImpl db, Cleaner cleaner):
		hook_checkDeletedDb(db, cleaner){
		/*
		 * If the DB is gone, this LN is obsolete. If delete cleanup is in
		 * progress, put the DB into the DB pending set; this LN will be
		 * declared deleted after the delete cleanup is finished.
		 */
		if (db == null || db.isDeleted()) {
			cleaner.addPendingDB(db);
			hook_nLNsDead(cleaner);

			return true;
		}
		return false;
	}

	boolean around(DatabaseImpl db):
		hook_isDbGone(db) {
		return proceed(db) || db.isDeleted();
	}

	before(CheckpointReference targetRef, SortedMap dirtyMap,
			boolean allowDeltas, long checkpointStart, Checkpointer checkpointer)
			throws DatabaseException:
			hook_checkDeleted(targetRef, dirtyMap, allowDeltas, checkpointStart, checkpointer) {
		/*
		 * Check if the db is still valid since INs of deleted databases are
		 * left on the in-memory tree until the post transaction cleanup is
		 * finished.
		 */
		if (!(targetRef.db.isDeleted())) {
			Integer currentLevel = (Integer) dirtyMap.firstKey();
			boolean logProvisionally = (currentLevel.intValue() != checkpointer.highestFlushLevel);
			checkpointer.flushIN(targetRef, dirtyMap, currentLevel.intValue(),
					logProvisionally, allowDeltas, checkpointStart);
		}
	}

	after(Database db) throws DatabaseException:
		checkRequiredDbState(db){
		db.databaseImpl.checkIsDeleted("preload");
	}

	boolean around(DatabaseImpl database):
		hook_getDbExistsEnvironment(database) {
		if (!proceed(database))
			return false;
		return !database.isDeleted();
	}

	boolean around(DatabaseImpl db, IN in) throws DatabaseException:
		hook_checkDeletedEvictor(db, in){
		/*
		 * We don't expect to see an IN with a database that has finished delete
		 * processing, because it would have been removed from the inlist during
		 * post-delete cleanup.
		 */
		if (db == null || db.isDeleteFinished()) {
			String inInfo = " IN type=" + in.getLogType() + " id="
					+ in.getNodeId() + " not expected on INList";
			String errMsg = (db == null) ? inInfo : "Database "
					+ db.getDebugName() + " id=" + db.getId() + inInfo;
			throw new DatabaseException(errMsg);
		}

		/* Ignore if the db is in the middle of delete processing. */
		if (db.isDeleted()) {
			return true;
		}
		return false;
	}

	boolean around(DatabaseImpl database) :
		hook_getDbExistsINCompressor(database) {
		if (!proceed(database))
			return false;
		return !database.isDeleted();
	}

	/*
	 * Delete processing states. See design note on database deletion and
	 * truncation
	 */
	private static final short NOT_DELETED = 1;

	private static final short DELETED_CLEANUP_INLIST_HARVEST = 2;

	private static final short DELETED_CLEANUP_LOG_HARVEST = 3;

	private static final short DELETED = 4;

	private short DatabaseImpl.deleteState; // one of four delete states.

	/*
	 * @return true if this database is deleted. Delete cleanup may still be in
	 * progress.
	 */
	public boolean DatabaseImpl.isDeleted() {
		return !(deleteState == NOT_DELETED);
	}

	/*
	 * @return true if this database is deleted and all cleanup is finished.
	 */
	public boolean DatabaseImpl.isDeleteFinished() {
		return (deleteState == DELETED);
	}

	/*
	 * The delete cleanup is starting. Set this before releasing any write locks
	 * held for a db operation.
	 */
	public void DatabaseImpl.startDeleteProcessing() {
		assert (deleteState == NOT_DELETED);

		deleteState = DELETED_CLEANUP_INLIST_HARVEST;
	}

	/*
	 * Should be called by the SortedLSNTreeWalker when it is finished with the
	 * INList.
	 */
	public void DatabaseImpl.finishedINListHarvest() {
		assert (deleteState == DELETED_CLEANUP_INLIST_HARVEST);

		deleteState = DELETED_CLEANUP_LOG_HARVEST;
	}

	/**
	 * Purge a DatabaseImpl and corresponding MapLN in the db mapping tree.
	 * Purging consists of removing all related INs from the db mapping tree and
	 * deleting the related MapLN. Used at the a transaction end in these cases:
	 * - purge the deleted database after a commit of Environment.removeDatabase
	 * - purge the deleted database after a commit of
	 * Environment.truncateDatabase - purge the newly created database after an
	 * abort of Environment.truncateDatabase
	 */
	public void DatabaseImpl.deleteAndReleaseINs() throws DatabaseException {

		startDeleteProcessing();
		releaseDeletedINs();
	}

	/* Mark each LSN obsolete in the utilization tracker. */
	private static class ObsoleteProcessor implements TreeNodeProcessor {

		private UtilizationTracker tracker;

		ObsoleteProcessor(UtilizationTracker tracker) {
			this.tracker = tracker;
		}

		public void processLSN(long childLsn, LogEntryType childType) {
			assert childLsn != DbLsn.NULL_LSN;
			tracker.countObsoleteNodeInexact(childLsn, childType);
		}
	}

	public void DatabaseImpl.releaseDeletedINs() throws DatabaseException {

		if (pendingDeletedHook != null) {
			pendingDeletedHook.doHook();
		}

		try {
			/*
			 * Get the root lsn before deleting the MapLN, as that will null out
			 * the root.
			 */
			long rootLsn = tree.getRootLsn();
			if (rootLsn == DbLsn.NULL_LSN) {

				/*
				 * There's nothing in this database. (It might be the abort of a
				 * truncation, where we are trying to clean up the new, blank
				 * database. Do delete the MapLN.
				 */
				envImpl.getDbMapTree().deleteMapLN(id);

			} else {

				UtilizationTracker snapshot = new UtilizationTracker(envImpl);

				/* Start by recording the lsn of the root IN as obsolete. */
				snapshot.countObsoleteNodeInexact(rootLsn, LogEntryType.LOG_IN);

				/* Use the tree walker to visit every child lsn in the tree. */
				ObsoleteProcessor obsoleteProcessor = new ObsoleteProcessor(
						snapshot);
				SortedLSNTreeWalker walker = new SetDbStateSortedLSNTreeWalker(
						this, true, // remove INs from INList
						rootLsn, obsoleteProcessor);
				/*
				 * Delete MapLN before the walk. Note that the processing of the
				 * naming tree means this MapLN is never actually accessible
				 * from the current tree, but deleting the MapLN will do two
				 * things: (a) mark it properly obsolete (b) null out the
				 * database tree, leaving the INList the only reference to the
				 * INs.
				 */
				envImpl.getDbMapTree().deleteMapLN(id);

				/*
				 * At this point, it's possible for the evictor to find an IN
				 * for this database on the INList. It should be ignored.
				 */
				walker.walk();

				/*
				 * Count obsolete nodes for a deleted database at transaction
				 * end time. Write out the modified file summaries for recovery.
				 */
				envImpl.getUtilizationProfile().countAndLogSummaries(
						snapshot.getTrackedFiles());
			}
		} finally {
			deleteState = DELETED;
		}
	}

	public void DatabaseImpl.checkIsDeleted(String operation)
			throws DatabaseException {

		if (isDeleted()) {
			throw new DatabaseException("Attempt to " + operation
					+ " a deleted database");
		}
	}

	/**
	 * Javadoc for this public method is generated via the doc templates in the
	 * doc_src directory.
	 */
	public void Environment.removeDatabase(OperationContext cxt,
			String databaseName) throws DatabaseException {

		checkHandleIsValid();
		checkEnv();
		DatabaseUtil.checkForNullParam(databaseName, "databaseName");

		Locker locker = null;
		boolean operationOk = false;
		try {

			/*
			 * Note: use env level isTransactional as proxy for the db
			 * isTransactional.
			 */
			locker = LockerFactory
					.getWritableLocker(this, cxt, true /* retainNonTxnLocks */);
			environmentImpl.dbRemove(locker, databaseName);
			operationOk = true;
		} finally {
			if (locker != null) {
				locker.operationEnd(operationOk);
			}
		}
	}

	/**
	 * Remove a database.
	 */
	public void EnvironmentImpl.dbRemove(Locker locker, String databaseName)
			throws DatabaseException {

		dbMapTree.dbRemove(locker, databaseName);
	}

	/**
	 * Remove the database by deleting the nameLN.
	 */
	void DbTree.dbRemove(Locker locker, String databaseName)
			throws DatabaseException {

		CursorImpl nameCursor = null;
		try {
			NameLockResult result = lockNameLN(locker, databaseName, "remove");
			nameCursor = result.nameCursor;
			if (nameCursor == null) {
				return;
			} else {

				/*
				 * Delete the NameLN. There's no need to mark any Database
				 * handle invalid, because the handle must be closed when we
				 * take action and any further use of the handle will re-look up
				 * the database.
				 */
				nameCursor.delete();

				/*
				 * Schedule database for final deletion during commit. This
				 * should be the last action taken, since this will take effect
				 * immediately for non-txnal lockers.
				 */
				locker.markDeleteAtTxnEnd(result.dbImpl, true);
			}
		} finally {
			if (nameCursor != null) {
				nameCursor.close();
			}
		}
	}

	// needed to be advisable
	private void hook_nLNsDead(Cleaner cleaner) {
	}

	/*
	 * We have to keep a set of DatabaseCleanupInfo objects so after commit or
	 * abort of Environment.truncateDatabase() or Environment.removeDatabase(),
	 * we can appropriately purge the unneeded MapLN and DatabaseImpl.
	 * Synchronize access to this set on this object.
	 */
	private Set Txn.deletedDatabases;

	/**
	 * @param dbImpl
	 *            databaseImpl to remove
	 * @param deleteAtCommit
	 *            true if this databaseImpl should be cleaned on commit, false
	 *            if it should be cleaned on abort.
	 * @param mb
	 *            environment memory budget.
	 */
	public void Txn.markDeleteAtTxnEnd(DatabaseImpl dbImpl,
			boolean deleteAtCommit) throws DatabaseException {

		synchronized (this) {
			int delta = 0;
			if (deletedDatabases == null) {
				deletedDatabases = new HashSet();
				delta += MemoryBudget.HASHSET_OVERHEAD;
			}

			deletedDatabases
					.add(new DatabaseCleanupInfo(dbImpl, deleteAtCommit));
			delta += MemoryBudget.HASHSET_ENTRY_OVERHEAD
					+ MemoryBudget.OBJECT_OVERHEAD;
			updateMemoryUsage(delta);
		}
	}

	/**
	 * Database operations like remove and truncate leave behind residual
	 * DatabaseImpls that must be purged at transaction commit or abort.
	 */
	public abstract void Locker.markDeleteAtTxnEnd(DatabaseImpl db,
			boolean deleteAtCommit) throws DatabaseException;

	public void BasicLocker.markDeleteAtTxnEnd(DatabaseImpl db,
			boolean deleteAtCommit) throws DatabaseException {

		if (deleteAtCommit) {
			db.deleteAndReleaseINs();
		}
	}

	public void BuddyLocker.markDeleteAtTxnEnd(DatabaseImpl db,
			boolean deleteAtCommit) throws DatabaseException {
		if (deleteAtCommit) {
			db.deleteAndReleaseINs();
		}
	}

}
