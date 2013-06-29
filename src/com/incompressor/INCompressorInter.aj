package com.incompressor;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.cleaner.UtilizationTracker;
import com.sleepycat.je.config.BooleanConfigParam;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.config.IntConfigParam;
import com.sleepycat.je.config.LongConfigParam;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.LogManager;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.tree.INDeleteInfo;
import com.sleepycat.je.tree.Tree;

public privileged aspect INCompressorInter {

	/*
	 * IN Compressor
	 */
	public static final LongConfigParam EnvironmentParams.COMPRESSOR_WAKEUP_INTERVAL = new LongConfigParam(
			"je.compressor.wakeupInterval", new Long(1000000), // min
			new Long(4294967296L), // max
			new Long(5000000), // default
			false, // mutable
			"# The compressor wakeup interval in microseconds.");

	public static final IntConfigParam EnvironmentParams.COMPRESSOR_RETRY = new IntConfigParam(
			"je.compressor.deadlockRetry", new Integer(0), // min
			new Integer(Integer.MAX_VALUE),// max
			new Integer(3), // default
			false, // mutable
			"# Number of times to retry a compression run if a deadlock occurs.");

	public static final LongConfigParam EnvironmentParams.COMPRESSOR_LOCK_TIMEOUT = new LongConfigParam(
			"je.compressor.lockTimeout", new Long(0), // min
			new Long(4294967296L), // max
			new Long(500000L), // default
			false, // mutable
			"# The lock timeout for compressor transactions in microseconds.");

	// changed to public
	public INCompressor EnvironmentImpl.inCompressor;

	/**
	 * Return the incompressor. In general, don't use this directly because it's
	 * easy to forget that the incompressor can be null at times (i.e during the
	 * shutdown procedure. Instead, wrap the functionality within this class,
	 * like lazyCompress.
	 */
	public INCompressor EnvironmentImpl.getINCompressor() {
		return inCompressor;
	}

	/**
	 * Invoke a compress programatically. Note that only one compress may run at
	 * a time.
	 */
	public boolean EnvironmentImpl.invokeCompressor() throws DatabaseException {

		if (inCompressor != null) {
			inCompressor.doCompress();
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Available for the unit tests.
	 */
	public void EnvironmentImpl.shutdownINCompressorDaemon()
			throws InterruptedException {

		if (inCompressor != null) {
			inCompressor.shutdown();

			/*
			 * If daemon thread doesn't shutdown for any reason, at least clear
			 * the reference to the environment so it can be GC'd.
			 */
			inCompressor.clearEnv();
			inCompressor = null;
		}
		return;
	}

	public int EnvironmentImpl.getINCompressorQueueSize()
			throws DatabaseException {

		return inCompressor.getBinRefQueueSize();
	}

	/**
	 * Javadoc for this public method is generated via the doc templates in the
	 * doc_src directory.
	 */
	public void Environment.compress() throws DatabaseException {

		checkHandleIsValid();
		checkEnv();
		environmentImpl.invokeCompressor();
	}
	
	public static final BooleanConfigParam EnvironmentParams.COMPRESSOR_PURGE_ROOT = new BooleanConfigParam(
			"je.compressor.purgeRoot", false, // default
			false, // mutable
			"# If true, when the compressor encounters an empty tree, the root\n"
					+ "# node of the tree is deleted.");

	
	
	/**
	 * This entire tree is empty, clear the root and log a new MapLN
	 * 
	 * @return the rootIN that has been detached, or null if there hasn't been
	 *         any removal.
	 */
	// changed to public
	public IN Tree.logTreeRemoval(IN rootIN, UtilizationTracker tracker)
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
