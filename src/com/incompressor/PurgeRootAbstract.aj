package com.incompressor;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.cleaner.UtilizationTracker;
import com.sleepycat.je.config.BooleanConfigParam;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.LogManager;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.tree.INDeleteInfo;
import com.sleepycat.je.tree.Tree;

public privileged aspect PurgeRootAbstract {

	pointcut setDatabase(DatabaseImpl database) : execution(void Tree.setDatabase(DatabaseImpl)) && args(database);

	pointcut hook_updateRoot(IN rootIN, UtilizationTracker tracker, Tree tree) : execution(IN Tree.hook_updateRoot(IN, UtilizationTracker)) && args(rootIN, tracker) && this(tree);
	
	after(DatabaseImpl database) throws DatabaseException:
		setDatabase(database){
		DbConfigManager configManager = database.getDbEnvironment()
				.getConfigManager();
		purgeRoot = configManager
				.getBoolean(EnvironmentParams.COMPRESSOR_PURGE_ROOT);
	}

	IN around(IN rootIN, UtilizationTracker tracker, Tree tree)
			throws DatabaseException:
				hook_updateRoot(rootIN, tracker, tree){
		IN subtreeRootIN = null;
		if (purgeRoot) {
			subtreeRootIN = tree.logTreeRemoval(rootIN, tracker);
		}
		return subtreeRootIN;
	}
	
	public static final BooleanConfigParam EnvironmentParams.COMPRESSOR_PURGE_ROOT = new BooleanConfigParam(
			"je.compressor.purgeRoot", false, // default
			false, // mutable
			"# If true, when the compressor encounters an empty tree, the root\n"
					+ "# node of the tree is deleted.");

	private boolean purgeRoot;
	
	/**
	 * This entire tree is empty, clear the root and log a new MapLN
	 * 
	 * @return the rootIN that has been detached, or null if there hasn't been
	 *         any removal.
	 */
	private IN Tree.logTreeRemoval(IN rootIN, UtilizationTracker tracker)
			throws DatabaseException {
		// Lck assert rootLatch.isWriteLockedByCurrentThread();
		IN detachedRootIN = null;

		/**
		 * XXX: Suspect that validateSubtree is no longer needed, now that we
		 * hold all latches.
		 */
		if ((rootIN.getNEntries() <= 1)
				&& (rootIN.validateSubtreeBeforeDelete(0))) {

			root = null;

			/*
			 * Record the root deletion for recovery. Do this within the root
			 * latch. We need to put this log entry into the log before another
			 * thread comes in and creates a new rootIN for this database.
			 * 
			 * For example, LSN 1000 IN delete info entry LSN 1010 new IN, for
			 * next set of inserts LSN 1020 new BIN, for next set of inserts.
			 * 
			 * The entry at 1000 is needed so that LSN 1010 will properly
			 * supercede all previous IN entries in the tree. Without the
			 * INDelete, we may not use the new root, because it has a different
			 * node id.
			 */
			EnvironmentImpl envImpl = database.getDbEnvironment();
			LogManager logManager = envImpl.getLogManager();

			logManager.log(new INDeleteInfo(rootIN.getNodeId(), rootIN
					.getIdentifierKey(), database.getId()));

			detachedRootIN = rootIN;
		}
		return detachedRootIN;
	}
}
