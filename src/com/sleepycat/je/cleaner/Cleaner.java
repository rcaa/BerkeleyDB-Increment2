/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002-2006
 *      Sleepycat Software.  All rights reserved.
 *
 * $Id: Cleaner.java,v 1.4.6.2.2.11 2006/10/27 21:24:14 ckaestne Exp $
 */

package com.sleepycat.je.cleaner;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Level;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.dbi.EnvConfigObserver;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.ChildReference;
import com.sleepycat.je.tree.DIN;
import com.sleepycat.je.tree.LN;
import com.sleepycat.je.tree.Tree;
import com.sleepycat.je.tree.TreeLocation;
import com.sleepycat.je.txn.BasicLocker;
import com.sleepycat.je.txn.LockGrantType;
import com.sleepycat.je.txn.LockResult;
import com.sleepycat.je.txn.LockType;
import com.sleepycat.je.utilint.DaemonRunner;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.PropUtil;

/**
 * The Cleaner is responsible for effectively garbage collecting the JE log. It
 * looks through log files and locates log records (IN's and LN's of all
 * flavors) that are superceded by later versions. Those that are "current" are
 * propagated to a newer log file so that older log files can be deleted.
 */
public class Cleaner implements EnvConfigObserver {
	/* From cleaner */
	static final String CLEAN_IN = "CleanIN:";

	static final String CLEAN_LN = "CleanLN:";

	static final String CLEAN_MIGRATE_LN = "CleanMigrateLN:";

	static final String CLEAN_PENDING_LN = "CleanPendingLN:";

	/**
	 * Whether to fetch LNs for files in the to-be-cleaned set during lazy
	 * migration. This is currently enabled because we do not support the
	 * dynamic addition of cleaner threads; that way, if the configured cleaner
	 * threads cannot keep up, we use proactive migration to keep up.
	 */
	static final boolean PROACTIVE_MIGRATION = true;

	/**
	 * Whether to update the IN generation count during searches. This is
	 * currently disabled because 1) we update the generation of the BIN when we
	 * set a MIGRATE flag and 2) if the BIN is not evicted its parents will not
	 * be, so not updating the generation during the search has no benefit. By
	 * not updating the generation during searches for which we do NOT set the
	 * MIGRATE flag, we avoid holding INs in the cache that are not needed for
	 * lazy migration. However, we do very few searches for obsolete LNs because
	 * the obsolete tracking info prevents this, so the benefit of not updating
	 * the generation during searches is questionable. In other words, changing
	 * this setting will have little effect.
	 */
	static final boolean UPDATE_GENERATION = false;

	/*
	 * Configuration parameters are non-private for use by FileProcessor,
	 * UtilizationTracker.
	 */
	// changed to public
	public long lockTimeout;

	int readBufferSize;

	boolean expunge;

	boolean clusterResident;

	boolean clusterAll;

	int maxBatchFiles;

	// Level detailedTraceLevel;

	long cleanerBytesInterval;

	boolean trackDetail;

	/**
	 * All files that are to-be-cleaning or being-cleaned. Used to perform
	 * proactive migration. Is read-only after assignment, so no synchronization
	 * is needed.
	 */
	Set mustBeCleanedFiles = Collections.EMPTY_SET;

	/**
	 * All files that are below the minUtilization threshold. Used to perform
	 * clustering migration. Is read-only after assignment, so no
	 * synchronization is needed.
	 */
	Set lowUtilizationFiles = Collections.EMPTY_SET;

	private String name;

	// changed to public
	public EnvironmentImpl env;

	UtilizationProfile profile;

	private UtilizationTracker tracker;

	// changed to public
	public FileSelector fileSelector;

	private Object deleteFileLock;

	public Cleaner(EnvironmentImpl env, String name) throws DatabaseException {

		this.env = env;
		this.name = name;
		tracker = new UtilizationTracker(env, this);
		profile = new UtilizationProfile(env, tracker);
		fileSelector = new FileSelector();
		deleteFileLock = new Object();

		/*
		 * The trackDetail property is immutable because of the complexity (if
		 * it were mutable) in determining whether to update the memory budget
		 * and perform eviction.
		 */
		trackDetail = env.getConfigManager().getBoolean(
				EnvironmentParams.CLEANER_TRACK_DETAIL);

		/* Initialize mutable properties and register for notifications. */
		envConfigUpdate(env.getConfigManager());
		env.addConfigObserver(this);
	}

	/**
	 * Process notifications of mutable property changes.
	 */
	public void envConfigUpdate(DbConfigManager cm) throws DatabaseException {

		lockTimeout = PropUtil.microsToMillis(cm
				.getLong(EnvironmentParams.CLEANER_LOCK_TIMEOUT));

		readBufferSize = cm.getInt(EnvironmentParams.CLEANER_READ_SIZE);
		if (readBufferSize <= 0) {
			readBufferSize = cm
					.getInt(EnvironmentParams.LOG_ITERATOR_READ_SIZE);
		}

		expunge = cm.getBoolean(EnvironmentParams.CLEANER_REMOVE);

		clusterResident = cm.getBoolean(EnvironmentParams.CLEANER_CLUSTER);

		clusterAll = cm.getBoolean(EnvironmentParams.CLEANER_CLUSTER_ALL);

		maxBatchFiles = cm.getInt(EnvironmentParams.CLEANER_MAX_BATCH_FILES);

		// TODO detailedTraceLevel = Tracer.parseLevel(env,
		// EnvironmentParams.JE_LOGGING_LEVEL_CLEANER);

		if (clusterResident && clusterAll) {
			throw new IllegalArgumentException("Both "
					+ EnvironmentParams.CLEANER_CLUSTER + " and "
					+ EnvironmentParams.CLEANER_CLUSTER_ALL
					+ " may not be set to true.");
		}

		cleanerBytesInterval = cm
				.getLong(EnvironmentParams.CLEANER_BYTES_INTERVAL);
		if (cleanerBytesInterval == 0) {
			cleanerBytesInterval = cm.getLong(EnvironmentParams.LOG_FILE_MAX) / 4;
		}
	}

	public UtilizationTracker getUtilizationTracker() {
		return tracker;
	}

	public UtilizationProfile getUtilizationProfile() {
		return profile;
	}

	/**
	 * Cleans selected files and returns the number of files cleaned. This
	 * method is not invoked by a deamon thread, it is programatically.
	 * 
	 * @param cleanMultipleFiles
	 *            is true to clean until we're under budget, or false to clean
	 *            at most one file.
	 * 
	 * @param forceCleaning
	 *            is true to clean even if we're not under the utilization
	 *            threshold.
	 * 
	 * @return the number of files cleaned, not including files cleaned
	 *         unsuccessfully.
	 */
	public int doClean(boolean cleanMultipleFiles, boolean forceCleaning)
			throws DatabaseException {

		FileProcessor processor = new FileProcessor("", env, this, profile,
				fileSelector);
		return processor.doClean(false /* invokedFromDaemon */,
				cleanMultipleFiles, forceCleaning);
	}

	/**
	 * Deletes all files that are safe-to-delete, if an exclusive lock on the
	 * environment can be obtained.
	 */
	void deleteSafeToDeleteFiles() throws DatabaseException {

		/*
		 * Synchronized to prevent multiple threads from requesting the same
		 * file lock.
		 */
		synchronized (deleteFileLock) {
			Set safeFiles = fileSelector.copySafeToDeleteFiles();
			if (safeFiles == null) {
				return; /* Nothing to do. */
			}

			/*
			 * Fail loudly if the environment is invalid. A RunRecoveryException
			 * must have occurred.
			 */
			env.checkIfInvalid();

			/*
			 * Fail silently if the environment is not open.
			 */
			if (env.mayNotWrite()) {
				return;
			}

			if (!hook_lockEnvironment())
				return;

			try {
				for (Iterator i = safeFiles.iterator(); i.hasNext();) {
					Long fileNum = (Long) i.next();
					hook_deleteFile(fileNum);

				}
			} finally {
				hook_releaseEnvironment();
			}
		}
	}

	private void hook_releaseEnvironment() throws DatabaseException {
	}

	private boolean hook_lockEnvironment() throws DatabaseException {
		return true;
	}

	private void hook_deleteFile(Long fileNum) throws DatabaseException {
		long fileNumValue = fileNum.longValue();
		boolean deleted = false;
		try {
			if (expunge) {
				env.getFileManager().deleteFile(fileNumValue);
			} else {
				env.getFileManager().renameFile(fileNumValue,
						FileManager.DEL_SUFFIX);
			}
			deleted = true;
		} catch (DatabaseException e) {
			traceFileNotDeleted(e, fileNumValue);
		} catch (IOException e) {
			traceFileNotDeleted(e, fileNumValue);
		}

		/*
		 * If the log file was not deleted, leave it in the safe-to-delete set
		 * (and the UP) so that we will retry the deletion later. If the log
		 * file was deleted, trace the deletion, delete the file from the UP and
		 * from the safe-to-delete set.
		 * 
		 * We do not retry if an error occurs deleting the UP database entries
		 * below. Retrying is intended only to solve a problem on Windows where
		 * deleting a log file isn't always possible immediately after closing
		 * it.
		 */
		if (deleted) {
			// refined trace
			hook_traceDeletedFile(fileNumValue);

			/*
			 * Remove the file from the profile before removing it from the
			 * safe-to-delete set. If we remove in the reverse order, it may be
			 * selected for cleaning. Always delete the file from the
			 * safe-to-delete set (in a finally block) so that we don't attempt
			 * to delete the file again.
			 */
			try {
				profile.removeFile(fileNum);
			} finally {
				fileSelector.removeDeletedFile(fileNum);
			}
		}

	}

	private void hook_traceDeletedFile(long fileNumValue) {
		// TODO Auto-generated method stub

	}

	private void traceFileNotDeleted(Exception e, long fileNum) {
		// refined trace Tracer.trace(env, "Cleaner",
		// "deleteSafeToDeleteFiles","Log file 0x" + Long.toHexString(fileNum) +
		// " could not be " +(expunge ? "deleted" : "renamed") +". This
		// operation will be retried at the next checkpoint.",e);
	}

	/**
	 * Returns a copy of the cleaned and processed files at the time a
	 * checkpoint starts.
	 * 
	 * <p>
	 * If non-null is returned, the checkpoint should flush an extra level, and
	 * addCheckpointedFiles() should be called when the checkpoint is complete.
	 * </p>
	 */
	public Set[] getFilesAtCheckpointStart() throws DatabaseException {

		/* Pending LNs can prevent file deletion. */
		processPending();

		return fileSelector.getFilesAtCheckpointStart();
	}

	/**
	 * When a checkpoint is complete, update the files that were returned at the
	 * beginning of the checkpoint.
	 */
	public void updateFilesAtCheckpointEnd(Set[] files)
			throws DatabaseException {

		fileSelector.updateFilesAtCheckpointEnd(files);
		deleteSafeToDeleteFiles();
	}

	/**
	 * Update the lowUtilizationFiles and mustBeCleanedFiles fields with new
	 * read-only collections, and update the backlog file count.
	 */
	public void updateReadOnlyFileCollections() {
		mustBeCleanedFiles = fileSelector.getMustBeCleanedFiles();
		lowUtilizationFiles = fileSelector.getLowUtilizationFiles();

	}

	/**
	 * If any LNs are pending, process them. This method should be called often
	 * enough to prevent the pending LN set from growing too large.
	 */
	void processPending() throws DatabaseException {

		DbTree dbMapTree = env.getDbMapTree();

		LNInfo[] pendingLNs = fileSelector.getPendingLNs();
		if (pendingLNs != null) {
			TreeLocation location = new TreeLocation();

			for (int i = 0; i < pendingLNs.length; i += 1) {
				LNInfo info = pendingLNs[i];

				DatabaseId dbId = info.getDbId();
				DatabaseImpl db = dbMapTree.getDb(dbId, lockTimeout);

				byte[] key = info.getKey();
				byte[] dupKey = info.getDupKey();
				LN ln = info.getLN();

				processPendingLN(ln, db, key, dupKey, location);
			}
		}

	}

	/**
	 * Processes a pending LN, getting the lock first to ensure that the
	 * overhead of retries is mimimal.
	 */
	private void processPendingLN(LN ln, DatabaseImpl db, byte[] key,
			byte[] dupKey, TreeLocation location) throws DatabaseException {

		boolean parentFound = false; // We found the parent BIN.
		boolean processedHere = true; // The LN was cleaned here.
		boolean lockDenied = false; // The LN lock was denied.
		boolean obsolete = false; // The LN is no longer in use.
		boolean completed = false; // This method completed.

		BasicLocker locker = null;
		BIN bin = null;
		DIN parentDIN = null;
		try {

			if (hook_checkDeletedDb(db)) {
				obsolete = true;
				completed = true;
				return;
			}

			Tree tree = db.getTree();
			assert tree != null;

			/* Get a non-blocking lock on the original node ID. */

			locker = new BasicLocker(env);
			LockResult lockRet = locker.nonBlockingLock(ln.getNodeId(),
					LockType.READ, db);
			if (lockRet.getLockGrant() == LockGrantType.DENIED) {
				/* Try again later. */

				lockDenied = true;
				completed = true;
				return;
			}

			/*
			 * Search down to the bottom most level for the parent of this LN.
			 * 
			 * We pass searchDupTree=true to search the dup tree by nodeID if
			 * necessary. This handles the case where dupKey is null because the
			 * pending entry was a deleted single-duplicate in a BIN.
			 */
			parentFound = tree.getParentBINForChildLN(location, key, dupKey,
					ln, false, // splitsAllowed
					true, // findDeletedEntries
					true, // searchDupTree
					UPDATE_GENERATION);
			bin = location.bin;
			int index = location.index;

			if (!parentFound) {

				obsolete = true;
				completed = true;
				return;
			}

			if (ln.containsDuplicates()) {
				/* Migrate a DupCountLN. */
				parentDIN = (DIN) bin.fetchTarget(index);
				// Lck parentDIN.latch(UPDATE_GENERATION);
				ChildReference dclRef = parentDIN.getDupCountLNRef();
				processedHere = false;
				migrateDupCountLN(db, dclRef.getLsn(), parentDIN, dclRef, true, // wasCleaned
						true, // isPending
						ln.getNodeId(), // lockedPendingNodeId
						CLEAN_PENDING_LN);
			} else {
				/* Migrate a plain LN. */
				processedHere = false;
				migrateLN(db, bin.getLsn(index), bin, index, true, // wasCleaned
						true, // isPending
						ln.getNodeId(), // lockedPendingNodeId
						CLEAN_PENDING_LN);
			}
			completed = true;
		} catch (DatabaseException DBE) {
			DBE.printStackTrace();
			// refined trace
			hook_traceprocessLN(DBE);
			throw DBE;
		} finally {
			// Lck if (parentDIN != null) {
			// parentDIN.releaseLatchIfOwner();
			// }
			//
			// if (bin != null) {
			// bin.releaseLatchIfOwner();
			// }

			if (locker != null) {
				locker.operationEnd();
			}

			/*
			 * If migrateLN was not called above, remove the pending LN and
			 * perform tracing in this method.
			 */
			if (processedHere) {
				if (completed && !lockDenied) {
					fileSelector.removePendingLN(ln.getNodeId());
				}
				// refined trace trace(detailedTraceLevel, CLEAN_PENDING_LN, ln,
				// DbLsn.NULL_LSN, completed, obsolete, false /*migrated*/);
			}
		}
	}

	private void hook_traceprocessLN(DatabaseException dbe) {
		// TODO Auto-generated method stub

	}

	public boolean hook_checkDeletedDb(DatabaseImpl db) {
		return false;
	}

	/**
	 * Returns whether the given BIN entry may be stripped by the evictor. True
	 * is always returned if the BIN is not dirty. False is returned if the BIN
	 * is dirty and the entry will be migrated soon.
	 */
	public boolean isEvictable(BIN bin, int index) {

		if (bin.getDirty()) {

			if (bin.getMigrate(index)) {
				return false;
			}

			boolean isResident = (bin.getTarget(index) != null);
			Long fileNum = new Long(DbLsn.getFileNumber(bin.getLsn(index)));

			if ((PROACTIVE_MIGRATION || isResident)
					&& mustBeCleanedFiles.contains(fileNum)) {
				return false;
			}

			if ((clusterAll || (clusterResident && isResident))
					&& lowUtilizationFiles.contains(fileNum)) {
				return false;
			}
		}

		return true;
	}

	/**
	 * This method should be called just before logging a BIN. LNs will be
	 * migrated if the MIGRATE flag is set, or if they are in a file to be
	 * cleaned, or if the LNs qualify according to the rules for cluster and
	 * clusterAll.
	 * 
	 * <p>
	 * On return this method guarantees that no MIGRATE flag will be set on any
	 * child entry. If this method is *not* called before logging a BIN, then
	 * the addPendingLNs method must be called.
	 * </p>
	 * 
	 * @param bin
	 *            is the latched BIN. The latch will not be released by this
	 *            method.
	 * 
	 * @param proactiveMigration
	 *            perform proactive migration if needed; this is false during a
	 *            split, to reduce the delay in the user operation.
	 */
	public void lazyMigrateLNs(final BIN bin, boolean proactiveMigration)
			throws DatabaseException {

		DatabaseImpl db = bin.getDatabase();

		boolean isBinInDupDb = db.getSortedDuplicates()
				&& !bin.containsDuplicates();

		/*
		 * For non-resident LNs, sort them by LSN before migrating them.
		 * Fetching in LSN order reduces physical disk I/O.
		 */
		Integer[] sortedIndices = null;
		int nSortedIndices = 0;
		int nEntries = bin.getNEntries();

		for (int index = 0; index < nEntries; index += 1) {

			boolean migrateFlag = bin.getMigrate(index);
			boolean isResident = (bin.getTarget(index) != null);
			long childLsn = bin.getLsn(index);

			if (shouldMigrateLN(migrateFlag, isResident, proactiveMigration,
					isBinInDupDb, childLsn)) {

				if (isResident) {
					migrateLN(db, childLsn, bin, index, migrateFlag, // wasCleaned
							false, // isPending
							0, // lockedPendingNodeId
							CLEAN_MIGRATE_LN);
				} else {
					if (sortedIndices == null) {
						sortedIndices = new Integer[nEntries];
					}
					sortedIndices[nSortedIndices++] = new Integer(index);
				}
			}
		}

		if (sortedIndices != null) {
			Arrays.sort(sortedIndices, 0, nSortedIndices, new Comparator() {
				public int compare(Object o1, Object o2) {
					int i1 = ((Integer) o1).intValue();
					int i2 = ((Integer) o2).intValue();
					return DbLsn.compareTo(bin.getLsn(i1), bin.getLsn(i2));
				}
			});
			for (int i = 0; i < nSortedIndices; i += 1) {
				int index = sortedIndices[i].intValue();
				long childLsn = bin.getLsn(index);
				boolean migrateFlag = bin.getMigrate(index);
				migrateLN(db, childLsn, bin, index, migrateFlag, // wasCleaned
						false, // isPending
						0, // lockedPendingNodeId
						CLEAN_MIGRATE_LN);
			}
		}
	}

	/**
	 * This method should be called just before logging a root DIN. The
	 * DupCountLN will be migrated if the MIGRATE flag is set, or if it is in a
	 * file to be cleaned, or if the LN qualifies according to the rules for
	 * cluster and clusterAll.
	 * 
	 * <p>
	 * On return this method guarantees that the MIGRATE flag will not be set on
	 * the child entry. If this method is *not* called before logging a root
	 * DIN, then the addPendingDupCountLN method must be called.
	 * </p>
	 * 
	 * @param din
	 *            is the latched DIN. The latch will not be released by this
	 *            method.
	 * 
	 * @param dclRef
	 *            is the reference to the DupCountLN.
	 * 
	 * @param proactiveMigration
	 *            perform proactive migration if needed; this is false during a
	 *            split, to reduce the delay in the user operation.
	 */
	public void lazyMigrateDupCountLN(DIN din, ChildReference dclRef,
			boolean proactiveMigration) throws DatabaseException {

		DatabaseImpl db = din.getDatabase();

		boolean migrateFlag = dclRef.getMigrate();
		boolean isResident = (dclRef.getTarget() != null);
		boolean isBinInDupDb = false;
		long childLsn = dclRef.getLsn();

		if (shouldMigrateLN(migrateFlag, isResident, proactiveMigration,
				isBinInDupDb, childLsn)) {

			migrateDupCountLN(db, childLsn, din, dclRef, migrateFlag, // wasCleaned
					false, // isPending
					0, // lockedPendingNodeId
					CLEAN_MIGRATE_LN);
		}
	}

	/**
	 * Returns whether an LN entry should be migrated. Updates stats.
	 * 
	 * @param migrateFlag
	 *            is whether the MIGRATE flag is set on the entry.
	 * 
	 * @param isResident
	 *            is whether the LN is currently resident.
	 * 
	 * @param proactiveMigration
	 *            perform proactive migration if needed; this is false during a
	 *            split, to reduce the delay in the user operation.
	 * 
	 * @param isBinInDupDb
	 *            is whether this is a BIN entry in a database with duplicates
	 *            enabled.
	 * 
	 * @param childLsn
	 *            is the LSN of the LN.
	 * 
	 * @return whether to migrate the LN.
	 */
	private boolean shouldMigrateLN(boolean migrateFlag, boolean isResident,
			boolean proactiveMigration, boolean isBinInDupDb, long childLsn) {
		boolean doMigration = false;

		if (migrateFlag) {

			/*
			 * Always try to migrate if the MIGRATE flag is set, since the LN
			 * has been processed. If we did not migrate it, we would have to
			 * add it to pending LN set.
			 */
			doMigration = true;

		} else if (!proactiveMigration || isBinInDupDb || env.isClosing()) {

			/*
			 * Do nothing if proactiveMigration is false, since all further
			 * migration is optional.
			 * 
			 * Do nothing if this is a BIN in a duplicate database. We must not
			 * fetch DINs, since this BIN may be about to be evicted. Fetching a
			 * DIN would add it as an orphan to the INList, plus an IN with
			 * non-LN children is not evictable.
			 * 
			 * Do nothing if the environment is shutting down and the MIGRATE
			 * flag is not set. Proactive migration during shutdown is
			 * counterproductive -- it prevents a short final checkpoint, and it
			 * does not allow more files to be deleted.
			 */

		} else {

			Long fileNum = new Long(DbLsn.getFileNumber(childLsn));

			if ((PROACTIVE_MIGRATION || isResident)
					&& mustBeCleanedFiles.contains(fileNum)) {

				/* Migrate because it will be cleaned soon. */
				doMigration = true;

			} else if ((clusterAll || (clusterResident && isResident))
					&& lowUtilizationFiles.contains(fileNum)) {

				/* Migrate for clustering. */
				doMigration = true;
			}
		}

		return doMigration;
	}

	/**
	 * Migrate an LN in the given BIN entry, if it is not obsolete. The BIN is
	 * latched on entry to this method and is left latched when it returns.
	 */
	private void migrateLN(DatabaseImpl db, long lsn, BIN bin, int index,
			boolean wasCleaned, boolean isPending, long lockedPendingNodeId,
			String cleanAction) throws DatabaseException {

		/* Status variables are used to generate debug tracing info. */
		boolean obsolete = false; // The LN is no longer in use.
		boolean migrated = false; // The LN was in use and is migrated.
		boolean lockDenied = false; // The LN lock was denied.
		boolean completed = false; // This method completed.
		boolean clearTarget = false; // Node was non-resident when called.

		/*
		 * If wasCleaned is false we don't count statistics unless we migrate
		 * the LN. This avoids double counting.
		 */
		BasicLocker locker = null;
		LN ln = null;

		try {

			/*
			 * Fetch the node, if necessary. If it was not resident and it is an
			 * evictable LN, we will clear it after we migrate it.
			 */
			if (!bin.isEntryKnownDeleted(index)) {
				ln = (LN) bin.getTarget(index);
				if (ln == null) {
					/* If fetchTarget returns null, a deleted LN was cleaned. */
					ln = (LN) bin.fetchTarget(index);
					clearTarget = !db.getId().equals(DbTree.ID_DB_ID);
				}
			}

			/* Don't migrate knownDeleted or deleted cleaned LNs. */
			if (ln == null) {
				if (wasCleaned) {
					hook_nLNsDead();
				}
				obsolete = true;
				completed = true;
				return;
			}

			/*
			 * Get a non-blocking read lock on the LN. A pending node is already
			 * locked, but that node ID may be different than the current LN's
			 * node if a slot is reused. We must lock the current node to guard
			 * against aborts.
			 */
			if (lockedPendingNodeId != ln.getNodeId()) {
				locker = new BasicLocker(env);
				LockResult lockRet = locker.nonBlockingLock(ln.getNodeId(),
						LockType.READ, db);
				if (lockRet.getLockGrant() == LockGrantType.DENIED) {

					/*
					 * LN is currently locked by another Locker, so we can't
					 * assume anything about the value of the LSN in the bin.
					 */
					if (wasCleaned) {
						hook_nLNsLocked();
					}
					lockDenied = true;
					completed = true;
					return;
				}
			}

			/* Don't migrate deleted LNs. */
			if (ln.isDeleted()) {
				bin.setKnownDeletedLeaveTarget(index);
				if (wasCleaned) {
					hook_nLNsDead();
				}
				obsolete = true;
				completed = true;
				return;
			}

			/*
			 * Once we have a lock, check whether the current LSN needs to be
			 * migrated. There is no need to migrate it if the LSN no longer
			 * qualifies for cleaning. The LSN could have been changed by an
			 * update or delete after we set the MIGRATE flag.
			 * 
			 * Note that we do not perform this optimization if the MIGRATE flag
			 * is not set, i.e, for clustering and proactive migration of
			 * resident LNs. For these cases, we checked the conditions for
			 * migration immediately before calling this method. Although the
			 * condition could change after locking, the window is small and a
			 * second check is not worthwhile.
			 */
			if (bin.getMigrate(index)) {
				Long fileNum = new Long(DbLsn.getFileNumber(lsn));
				if (!fileSelector.isFileCleaningInProgress(fileNum)) {
					obsolete = true;
					completed = true;
					if (wasCleaned) {
						hook_nLNsDead();
					}
					return;
				}
			}

			/* Migrate the LN. */
			byte[] key = getLNMainKey(bin, index);
			long newLNLsn = ln.log(env, db.getId(), key, lsn, locker);
			bin.updateEntry(index, newLNLsn);
			migrated = true;
			completed = true;
			return;
		} finally {
			if (isPending) {
				if (completed && !lockDenied) {
					fileSelector.removePendingLN(lockedPendingNodeId);
				}
			} else {

				/*
				 * If a to-be-migrated LN was not processed successfully, we
				 * must guarantee that the file will not be deleted and that we
				 * will retry the LN later. The retry information must be
				 * complete or we may delete a file later without processing all
				 * of its LNs.
				 */
				if (bin.getMigrate(index) && (!completed || lockDenied)) {

					byte[] key = getLNMainKey(bin, index);
					byte[] dupKey = getLNDupKey(bin, index, ln);
					fileSelector.addPendingLN(ln, db.getId(), key, dupKey);

					/* Wake up the cleaner thread to process pending LNs. */
					env.getUtilizationTracker().newPendingLN();

					/*
					 * If we need to retry, don't clear the target since we
					 * would only have to fetch it again soon.
					 */
					clearTarget = false;
				}
			}

			/*
			 * Always clear the migrate flag. If the LN could not be locked and
			 * the migrate flag was set, the LN will have been added to the
			 * pending LN set above.
			 */
			bin.setMigrate(index, false);

			/*
			 * If the node was originally non-resident, clear it now so that we
			 * don't create more work for the evictor and reduce the cache
			 * memory available to the application.
			 */
			if (clearTarget) {
				bin.updateEntry(index, null);
			}

			if (locker != null) {
				locker.operationEnd();
			}
			// refined trace trace(detailedTraceLevel, cleanAction, ln, lsn,
			// completed, obsolete, migrated);
		}
	}

	private void hook_nLNsDead() {
		// TODO Auto-generated method stub

	}

	/**
	 * Migrate the DupCountLN for the given DIN. The DIN is latched on entry to
	 * this method and is left latched when it returns.
	 */
	private void migrateDupCountLN(DatabaseImpl db, long lsn, DIN parentDIN,
			ChildReference dclRef, boolean wasCleaned, boolean isPending,
			long lockedPendingNodeId, String cleanAction)
			throws DatabaseException {

		/* Status variables are used to generate debug tracing info. */
		boolean obsolete = false; // The LN is no longer in use.
		boolean migrated = false; // The LN was in use and is migrated.
		boolean lockDenied = false; // The LN lock was denied.
		boolean completed = false; // This method completed.
		boolean clearTarget = false; // Node was non-resident when called.

		/*
		 * If wasCleaned is false we don't count statistics unless we migrate
		 * the LN. This avoids double counting.
		 */
		BasicLocker locker = null;
		LN ln = null;

		try {

			/*
			 * Fetch the node, if necessary. If it was not resident and it is an
			 * evictable LN, we will clear it after we migrate it.
			 */
			ln = (LN) dclRef.getTarget();
			if (ln == null) {
				ln = (LN) dclRef.fetchTarget(db, parentDIN);
				assert ln != null;
				clearTarget = !db.getId().equals(DbTree.ID_DB_ID);
			}

			/*
			 * Get a non-blocking read lock on the LN, if this is not an already
			 * locked pending node.
			 */
			if (lockedPendingNodeId != ln.getNodeId()) {
				locker = new BasicLocker(env);
				LockResult lockRet = locker.nonBlockingLock(ln.getNodeId(),
						LockType.READ, db);
				if (lockRet.getLockGrant() == LockGrantType.DENIED) {

					/*
					 * LN is currently locked by another Locker, so we can't
					 * assume anything about the value of the LSN in the bin.
					 */
					if (wasCleaned) {
						hook_nLNsLocked();
					}
					lockDenied = true;
					completed = true;
					return;
				}
			}

			/*
			 * Once we have a lock, check whether the current LSN needs to be
			 * migrated. There is no need to migrate it if the LSN no longer
			 * qualifies for cleaning.
			 */
			Long fileNum = new Long(DbLsn.getFileNumber(lsn));
			if (!fileSelector.isFileCleaningInProgress(fileNum)) {
				obsolete = true;
				completed = true;
				if (wasCleaned) {
					hook_nLNsDead();
				}
				return;
			}

			/* Migrate the LN. */
			byte[] key = parentDIN.getDupKey();
			long newLNLsn = ln.log(env, db.getId(), key, lsn, locker);
			parentDIN.updateDupCountLNRef(newLNLsn);

			migrated = true;
			completed = true;
			return;
		} finally {
			if (isPending) {
				if (completed && !lockDenied) {
					fileSelector.removePendingLN(lockedPendingNodeId);
				}
			} else {

				/*
				 * If a to-be-migrated LN was not processed successfully, we
				 * must guarantee that the file will not be deleted and that we
				 * will retry the LN later. The retry information must be
				 * complete or we may delete a file later without processing all
				 * of its LNs.
				 */
				if (dclRef.getMigrate() && (!completed || lockDenied)) {

					byte[] key = parentDIN.getDupKey();
					byte[] dupKey = null;
					fileSelector.addPendingLN(ln, db.getId(), key, dupKey);

					/* Wake up the cleaner thread to process pending LNs. */
					env.getUtilizationTracker().newPendingLN();

					/*
					 * If we need to retry, don't clear the target since we
					 * would only have to fetch it again soon.
					 */
					clearTarget = false;
				}
			}

			/*
			 * Always clear the migrate flag. If the LN could not be locked and
			 * the migrate flag was set, the LN will have been added to the
			 * pending LN set above.
			 */
			dclRef.setMigrate(false);

			/*
			 * If the node was originally non-resident, clear it now so that we
			 * don't create more work for the evictor and reduce the cache
			 * memory available to the application.
			 */
			if (clearTarget) {
				parentDIN.updateDupCountLN(null);
			}

			if (locker != null) {
				locker.operationEnd();
			}
			// refined trace trace(detailedTraceLevel, cleanAction, ln, lsn,
			// completed, obsolete, migrated);
		}
	}

	private void hook_nLNsLocked() {
		// TODO Auto-generated method stub

	}

	/**
	 * Returns the main key for a given BIN entry.
	 */
	private byte[] getLNMainKey(BIN bin, int index) throws DatabaseException {

		if (bin.containsDuplicates()) {
			return bin.getDupKey();
		} else {
			return bin.getKey(index);
		}
	}

	/**
	 * Returns the duplicate key for a given BIN entry.
	 */
	private byte[] getLNDupKey(BIN bin, int index, LN ln)
			throws DatabaseException {

		DatabaseImpl db = bin.getDatabase();

		if (!db.getSortedDuplicates() || ln.containsDuplicates()) {

			/*
			 * The dup key is not needed for a non-duplicate DB or for a
			 * DupCountLN.
			 */
			return null;

		} else if (bin.containsDuplicates()) {

			/* The DBIN entry key is the dup key. */
			return bin.getKey(index);

		} else {

			/*
			 * The data is the dup key if the LN is not deleted. If the LN is
			 * deleted, this method will return null and we will do a node ID
			 * search later when processing the pending LN.
			 */
			return ln.getData();
		}
	}

	/**
	 * Adds the DB ID to the pending DB set if it is being deleted but deletion
	 * is not yet complete.
	 */
	// changed to public
	public void addPendingDB(DatabaseImpl db) {
		// refined trace
		/*
		 * if (db != null && db.isDeleted() && !db.isDeleteFinished()) {
		 * DatabaseId id = db.getId(); if (fileSelector.addPendingDB(id)) {
		 * Tracer.trace (detailedTraceLevel, env, "CleanAddPendingDB " + id); } }
		 */
	}

	/**
	 * Send trace messages to the java.util.logger. Don't rely on the logger
	 * alone to conditionalize whether we send this message, we don't even want
	 * to construct the message if the level is not enabled.
	 */
	// refined trace
	/*
	 * void trace(Level level, String action, Node node, long logLsn, boolean
	 * completed, boolean obsolete, boolean dirtiedMigrated) {
	 * 
	 * Logger logger = env.getLogger(); if (logger.isLoggable(level)) {
	 * StringBuffer sb = new StringBuffer(); sb.append(action); if (node !=
	 * null) { sb.append(" node="); sb.append(node.getNodeId()); } sb.append("
	 * logLsn="); sb.append(DbLsn.getNoFormatString(logLsn)); sb.append("
	 * complete=").append(completed); sb.append(" obsolete=").append(obsolete);
	 * sb.append(" dirtiedOrMigrated=").append(dirtiedMigrated);
	 * 
	 * logger.log(level, sb.toString()); } }
	 */
}
