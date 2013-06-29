package com.deletedb;

import java.util.SortedMap;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.cleaner.Cleaner;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.recovery.Checkpointer;
import com.sleepycat.je.recovery.Checkpointer.CheckpointReference;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.txn.Txn;

@Aspect
public class DeleteDBFeature {

	@Pointcut("execution(com.sleepycat.je.dbi.DatabaseImpl.new(..)) && this(db)")
	public void databaseConstructor(DatabaseImpl db) {
	}

	@After("databaseConstructor(db)")
	public void after1(DatabaseImpl db) {
		db.deleteState = DatabaseDeleteOperationInter.NOT_DELETED;
	}

	@Pointcut("execution(void com.sleepycat.je.cleaner.Cleaner.processPending()) && this(cleaner)")
	public void processPending(Cleaner cleaner) {
	}

	@After("processPending(cleaner)")
	public void after2(Cleaner cleaner) throws DatabaseException {
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

	@Pointcut("execution(boolean com.sleepycat.je.cleaner.Cleaner.hook_checkDeletedDb(com.sleepycat.je.dbi.DatabaseImpl)) && args(db) && target(cleaner)")
	public void hook_checkDeletedDb(DatabaseImpl db, Cleaner cleaner) {
	}

	@Around("hook_checkDeletedDb(db, cleaner)")
	public boolean around1(DatabaseImpl db, Cleaner cleaner) {
		/*
		 * If the DB is gone, this LN is obsolete. If delete cleanup is in
		 * progress, put the DB into the DB pending set; this LN will be
		 * declared deleted after the delete cleanup is finished.
		 */
		if (db == null || db.isDeleted()) {
			cleaner.addPendingDB(db);
			// hook_nLNsDead(cleaner);

			return true;
		}
		return false;
	}

	@Pointcut("execution(boolean com.sleepycat.je.cleaner.UtilizationProfile.hook_isDbGone(com.sleepycat.je.dbi.DatabaseImpl)) && args(db)")
	public void hook_isDbGone(DatabaseImpl db) {
	}

	@Around("hook_isDbGone(db)")
	public boolean around2(DatabaseImpl db, ProceedingJoinPoint pjp)
			throws Throwable {
		Object[] objs = new Object[] { db };
		return (Boolean) pjp.proceed(objs) || db.isDeleted();
	}

	@Pointcut("execution(void com.sleepycat.je.recovery.Checkpointer.hook_checkDeleted(com.sleepycat.je.recovery.Checkpointer.CheckpointReference, java.util.SortedMap, boolean, long)) && args(targetRef, dirtyMap, allowDeltas, checkpointStart) && this(checkpointer)")
	public void hook_checkDeleted(CheckpointReference targetRef,
			SortedMap dirtyMap, boolean allowDeltas, long checkpointStart,
			Checkpointer checkpointer) {
	}

	@Before("hook_checkDeleted(targetRef, dirtyMap, allowDeltas, checkpointStart, checkpointer)")
	public void before1(CheckpointReference targetRef, SortedMap dirtyMap,
			boolean allowDeltas, long checkpointStart, Checkpointer checkpointer)
			throws DatabaseException {
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

	@Pointcut("call(void checkRequiredDbState(com.sleepycat.je.Database.DbState, String)) && withincode(* com.sleepycat.je.Database.preload(..)) && this(db)")
	public void checkRequiredDbState(Database db) {
	}

	@After("checkRequiredDbState(db)")
	public void after3(Database db) throws DatabaseException {
		db.databaseImpl.checkIsDeleted("preload");
	}
	
	@Pointcut("execution(boolean com.sleepycat.je.Environment.hook_getDbExists(com.sleepycat.je.dbi.DatabaseImpl)) && args(database)")
	public void hook_getDbExistsEnvironment(DatabaseImpl database) {}
	
	@Around("hook_getDbExistsEnvironment(database)")
	public boolean around3(DatabaseImpl database, ProceedingJoinPoint pjp) throws Throwable {
		Object[] objs = new Object[] {database};
		if (!(Boolean)pjp.proceed(objs))
			return false;
		return !database.isDeleted();
	}
	
	@Pointcut("execution(boolean com.evictor.Evictor.hook_checkDeleted(com.sleepycat.je.dbi.DatabaseImpl, com.sleepycat.je.tree.IN)) && args(db, in)")
	public void hook_checkDeletedEvictor(DatabaseImpl db, IN in) {}
	
	@Around("hook_checkDeletedEvictor(db, in)")
	public boolean around4(DatabaseImpl db, IN in) throws DatabaseException {
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
	
	@Pointcut("execution(boolean com.incompressor.INCompressor.hook_getDbExists(com.sleepycat.je.dbi.DatabaseImpl)) && args(database)")
	public void hook_getDbExistsINCompressor(DatabaseImpl database) {}
	
	@Around("hook_getDbExistsINCompressor(database)")
	public boolean around5(DatabaseImpl database, ProceedingJoinPoint pjp) throws Throwable {
		Object[] objs = new Object[] {database};
		if (!(Boolean)pjp.proceed(objs))
			return false;
		return !database.isDeleted();
	}
	
	@Pointcut("call(void com.sleepycat.je.txn.Txn.hook_commitDeletedDatabaseState()) && this(txn)")
	public void hook_commitDeletedDatabaseState(Txn txn) {}
	
	@After("hook_commitDeletedDatabaseState(txn)")
	public void after4(Txn txn) throws DatabaseException {
		/*
		 * Set database state for deletes before releasing any write locks.
		 */
		txn.setDeletedDatabaseState(true);
	}
	
	@Pointcut("call(void com.sleepycat.je.txn.Txn.hook_abortDeletedDatabaseState()) && this(txn)")
	public void hook_abortDeletedDatabaseState(Txn txn) {}
	
	@After("hook_abortDeletedDatabaseState(txn)")
	public void after5(Txn txn) throws DatabaseException {
		/*
		 * Set database state for deletes before releasing any write locks.
		 */
		txn.setDeletedDatabaseState(false);
	}
	
	@Pointcut("call(void com.sleepycat.je.txn.Txn.close(boolean)) && withincode(long com.sleepycat.je.txn.Txn.commit(..)) && this(txn)")
	public void close(Txn txn) {}
	
	@Before("close(txn)")
	public void before2(Txn txn) throws DatabaseException {
		/*
		 * Purge any databaseImpls not needed as a result of the commit. Be sure
		 * to do this outside the synchronization block, to avoid conflict
		 * w/checkpointer.
		 */
		txn.cleanupDatabaseImpls(true);
	}
	
	@Pointcut("call(void com.sleepycat.je.txn.Txn.hook_cleanupDatabaseImpls()) && withincode(* com.sleepycat.je.txn.Txn.abortInternal(..)) && this(txn)")
	public void hook_cleanupDatabaseImpls(Txn txn) {}
	
	@Before("hook_cleanupDatabaseImpls(txn)")
	public void before3(Txn txn) throws DatabaseException {
		/*
		 * Purge any databaseImpls not needed as a result of the abort. Be sure
		 * to do this outside the synchronization block, to avoid conflict
		 * w/checkpointer.
		 */
		txn.cleanupDatabaseImpls(false);
	}
	
}
