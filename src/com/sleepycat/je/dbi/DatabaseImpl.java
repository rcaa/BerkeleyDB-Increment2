/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002-2006
 *      Sleepycat Software.  All rights reserved.
 *
 * $Id: DatabaseImpl.java,v 1.1.6.3.2.7 2007/03/01 23:15:32 ckaestne Exp $
 */

package com.sleepycat.je.dbi;


import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.PreloadConfig;
import com.sleepycat.je.PreloadResult;
import com.sleepycat.je.PreloadStatus;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.cleaner.UtilizationTracker;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.SortedLSNTreeWalker.TreeNodeProcessor;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogException;
import com.sleepycat.je.log.LogReadable;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.LogWritable;
import com.sleepycat.je.tree.ChildReference;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.tree.Node;
import com.sleepycat.je.tree.Tree;
import com.sleepycat.je.tree.TreeUtils;
import com.sleepycat.je.tree.WithRootLatched;
import com.sleepycat.je.txn.Locker;
import com.sleepycat.je.txn.ThreadLocker;
import com.sleepycat.je.utilint.CmdUtil;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.TestHook;

/**
 * The underlying object for a given database.
 */
public class DatabaseImpl implements LogWritable, LogReadable, Cloneable {



	private DatabaseId id; // unique id

	private Tree tree;

	private EnvironmentImpl envImpl; // Tree operations find the env this way

	private boolean duplicatesAllowed; // duplicates allowed


	private Set referringHandles; // Set of open Database handles


	private long eofNodeId; // Logical EOF node for range locking

	/*
	 * The user defined Btree and duplicate comparison functions, if specified.
	 */
	private Comparator btreeComparator = null;

	private Comparator duplicateComparator = null;

	private String btreeComparatorName = "";

	private String duplicateComparatorName = "";

	/*
	 * Cache some configuration values.
	 */
	private int binDeltaPercent;

	private int binMaxDeltas;

	private int maxMainTreeEntriesPerNode;

	private int maxDupTreeEntriesPerNode;

	/*
	 * The debugDatabaseName is used for error messages only, to avoid accessing
	 * the db mapping tree in error situations. Currently it's not guaranteed to
	 * be transactionally correct, nor is it updated by rename.
	 */
	private String debugDatabaseName;

	/* For unit tests */
	private TestHook pendingDeletedHook;

	/**
	 * Create a database object for a new database.
	 */
	public DatabaseImpl(String dbName, DatabaseId id, EnvironmentImpl envImpl,
			DatabaseConfig dbConfig) throws DatabaseException {

		this.id = id;
		this.envImpl = envImpl;
		this.btreeComparator = dbConfig.getBtreeComparator();
		this.duplicateComparator = dbConfig.getDuplicateComparator();
		duplicatesAllowed = dbConfig.getSortedDuplicates();
		maxMainTreeEntriesPerNode = dbConfig.getNodeMaxEntries();
		maxDupTreeEntriesPerNode = dbConfig.getNodeMaxDupTreeEntries();

		initDefaultSettings();


		/*
		 * The tree needs the env, make sure we assign it before allocating the
		 * tree.
		 */
		tree = new Tree(this);
		referringHandles = Collections.synchronizedSet(new HashSet());

		eofNodeId = Node.getNextNodeId();

		/* For error messages only. */
		debugDatabaseName = dbName;
	}

	/**
	 * Create an empty database object for initialization from the log. Note
	 * that the rest of the initialization comes from readFromLog(), except for
	 * the debugDatabaseName, which is set by the caller.
	 */
	public DatabaseImpl() throws DatabaseException {

		id = new DatabaseId();
		envImpl = null;


		tree = new Tree();
		referringHandles = Collections.synchronizedSet(new HashSet());

		/* initDefaultSettings is called after envImpl is set. */

		eofNodeId = Node.getNextNodeId();
	}

	public void setDebugDatabaseName(String debugName) {
		debugDatabaseName = debugName;
	}

	public String getDebugName() {
		return debugDatabaseName;
	}

	/* For unit testing only. */
	public void setPendingDeletedHook(TestHook hook) {
		pendingDeletedHook = hook;
	}

	/**
	 * Initialize configuration settings when creating a new instance or after
	 * reading an instance from the log. The envImpl field must be set before
	 * calling this method.
	 */
	private void initDefaultSettings() throws DatabaseException {

		DbConfigManager configMgr = envImpl.getConfigManager();

		binDeltaPercent = configMgr.getInt(EnvironmentParams.BIN_DELTA_PERCENT);
		binMaxDeltas = configMgr.getInt(EnvironmentParams.BIN_MAX_DELTAS);

		if (maxMainTreeEntriesPerNode == 0) {
			maxMainTreeEntriesPerNode = configMgr
					.getInt(EnvironmentParams.NODE_MAX);
		}

		if (maxDupTreeEntriesPerNode == 0) {
			maxDupTreeEntriesPerNode = configMgr
					.getInt(EnvironmentParams.NODE_MAX_DUPTREE);
		}
	}

	/**
	 * Clone. For now just pass off to the super class for a field-by-field
	 * copy.
	 */
	public Object clone() throws CloneNotSupportedException {

		return super.clone();
	}

	/**
	 * @return the database tree.
	 */
	public Tree getTree() {
		return tree;
	}

	void setTree(Tree tree) {
		this.tree = tree;
	}

	/**
	 * @return the database id.
	 */
	public DatabaseId getId() {
		return id;
	}

	void setId(DatabaseId id) {
		this.id = id;
	}

	public long getEofNodeId() {
		return eofNodeId;
	}



	/**
	 * @return true if duplicates are allowed in this database.
	 */
	public boolean getSortedDuplicates() {
		return duplicatesAllowed;
	}

	public int getNodeMaxEntries() {
		return maxMainTreeEntriesPerNode;
	}

	public int getNodeMaxDupTreeEntries() {
		return maxDupTreeEntriesPerNode;
	}

	/**
	 * Set the duplicate comparison function for this database.
	 * 
	 * @param duplicateComparator -
	 *            The Duplicate Comparison function.
	 */
	public void setDuplicateComparator(Comparator duplicateComparator) {
		this.duplicateComparator = duplicateComparator;
	}

	/**
	 * Set the btree comparison function for this database.
	 * 
	 * @param btreeComparator -
	 *            The btree Comparison function.
	 */
	public void setBtreeComparator(Comparator btreeComparator) {
		this.btreeComparator = btreeComparator;
	}

	/**
	 * @return the btree Comparator object.
	 */
	public Comparator getBtreeComparator() {
		return btreeComparator;
	}

	/**
	 * @return the duplicate Comparator object.
	 */
	public Comparator getDuplicateComparator() {
		return duplicateComparator;
	}

	/**
	 * Set the db environment during recovery, after instantiating the database
	 * from the log
	 */
	public void setEnvironmentImpl(EnvironmentImpl envImpl)
			throws DatabaseException {

		this.envImpl = envImpl;
		initDefaultSettings();
		tree.setDatabase(this);
	}

	/**
	 * @return the database environment.
	 */
	public EnvironmentImpl getDbEnvironment() {
		return envImpl;
	}

	/**
	 * Returns whether one or more handles are open.
	 */
	public boolean hasOpenHandles() {
		return referringHandles.size() > 0;
	}

	/**
	 * Add a referring handle
	 */
	public void addReferringHandle(Database db) {
		referringHandles.add(db);
	}

	/**
	 * Decrement the reference count.
	 */
	public void removeReferringHandle(Database db) {
		referringHandles.remove(db);
	}

	/**
	 * @return the referring handle count.
	 */
	synchronized int getReferringHandleCount() {
		return referringHandles.size();
	}

	/**
	 * For this secondary database return the primary that it is associated
	 * with, or null if not associated with any primary. Note that not all
	 * handles need be associated with a primary.
	 */
	public Database findPrimaryDatabase() throws DatabaseException {

		for (Iterator i = referringHandles.iterator(); i.hasNext();) {
			Object obj = i.next();
			if (obj instanceof SecondaryDatabase) {
				return ((SecondaryDatabase) obj).getPrimaryDatabase();
			}
		}
		return null;
	}

	public String getName() throws DatabaseException {

		return envImpl.getDbMapTree().getDbName(id);
	}


	/*
	 * Count each valid record.
	 */
	private static class LNCounter implements TreeNodeProcessor {

		private long counter;

		public void processLSN(long childLsn, LogEntryType childType) {
			assert childLsn != DbLsn.NULL_LSN;

			if (childType.equals(LogEntryType.LOG_LN_TRANSACTIONAL)
					|| childType.equals(LogEntryType.LOG_LN)) {
				counter++;
			}
		}

		long getCount() {
			return counter;
		}
	}





	/**
	 * Prints the key and data, if available, for a BIN entry that could not be
	 * read/verified. Uses the same format as DbDump and prints both the hex and
	 * printable versions of the entries.
	 */
	private void printErrorRecord(PrintStream out, DatabaseEntry key,
			DatabaseEntry data) {

		byte[] bytes = key.getData();
		StringBuffer sb = new StringBuffer("Error Key ");
		if (bytes == null) {
			sb.append("UNKNOWN");
		} else {
			CmdUtil.formatEntry(sb, bytes, false);
			sb.append(' ');
			CmdUtil.formatEntry(sb, bytes, true);
		}
		out.println(sb);

		bytes = data.getData();
		sb = new StringBuffer("Error Data ");
		if (bytes == null) {
			sb.append("UNKNOWN");
		} else {
			CmdUtil.formatEntry(sb, bytes, false);
			sb.append(' ');
			CmdUtil.formatEntry(sb, bytes, true);
		}
		out.println(sb);
	}

	/**
	 * Undeclared exception used to throw through SortedLSNTreeWalker code when
	 * preload has either filled the user's max byte or time request.
	 */
	public static class HaltPreloadException extends RuntimeException {

		private PreloadStatus status;

		HaltPreloadException(PreloadStatus status) {
			super(status.toString());
			this.status = status;
		}

		PreloadStatus getStatus() {
			return status;
		}
	}

	private static final HaltPreloadException timeExceededPreloadException = new HaltPreloadException(
			PreloadStatus.EXCEEDED_TIME);



	/**
	 * The processLSN() code for PreloadLSNTreeWalker.
	 */
	//needed to make inner class public (was private) for AspectJ
	public static class PreloadProcessor implements TreeNodeProcessor {

		private EnvironmentImpl envImpl;

		private long targetTime;

		private PreloadResult preloadResult;


		PreloadProcessor(EnvironmentImpl envImpl, /*//M.Blong maxBytes,*/
				long targetTime, PreloadResult ret) {
			this.envImpl = envImpl;
//			this.maxBytes = maxBytes;//M.B
			this.targetTime = targetTime;
			this.preloadResult = ret;
		}

		/**
		 * Called for each LSN that the SortedLSNTreeWalker encounters.
		 */
		public void processLSN(long childLsn, LogEntryType childType) {
			assert childLsn != DbLsn.NULL_LSN;

			/*
			 * Check if we've exceeded either the max time or max bytes allowed
			 * for this preload() call.
			 */
			if (System.currentTimeMillis() > targetTime) {
				throw timeExceededPreloadException;
			}



		}
	}

	/*
	 * An extension of SortedLSNTreeWalker that provides an LSN to IN/index map.
	 * When an LSN is processed by the tree walker, the map is used to lookup
	 * the parent IN and child entry index of each LSN processed by the tree
	 * walker.
	 */
	public static class PreloadLSNTreeWalker extends SortedLSNTreeWalker {

		/* LSN -> INEntry */
		private Map lsnINMap = new HashMap();

		/* struct to hold IN/entry-index pair. */
		public static class INEntry {
			INEntry(IN in, int index) {
				this.in = in;
				this.index = index;
			}

			IN in;

			int index;
		}

		PreloadLSNTreeWalker(DatabaseImpl db, TreeNodeProcessor callback,
				PreloadConfig conf) throws DatabaseException {

			super(db, false, db.tree.getRootLsn(), callback);
			accumulateLNs = conf.getLoadLNs();
		}

		private final class PreloadWithRootLatched implements WithRootLatched {

			public IN doWork(ChildReference root) throws DatabaseException {

				walkInternal();
				return null;
			}
		}

		public void walk() throws DatabaseException {

			WithRootLatched preloadWRL = new PreloadWithRootLatched();
			dbImpl.getTree().withRoot(preloadWRL);
		}

		/*
		 * Method to get the Root IN for this DatabaseImpl's tree. Latches the
		 * root IN.
		 */
		protected IN getRootIN(long rootLsn) throws DatabaseException {

			return dbImpl.getTree().getRootIN(false);
		}



		/*
		 * Add an LSN -> IN/index entry to the map.
		 */
		protected void addToLsnINMap(Long lsn, IN in, int index) {
			assert in.getDatabase() != null;
			lsnINMap.put(lsn, new INEntry(in, index));
		}

		/*
		 * Process an LSN. Get & remove its INEntry from the map, then fetch the
		 * target at the INEntry's IN/index pair. This method will be called in
		 * sorted LSN order.
		 */
		protected Node fetchLSN(long lsn) throws DatabaseException {

			INEntry inEntry = (INEntry) lsnINMap.remove(new Long(lsn));
			assert (inEntry != null);
			IN in = inEntry.in;
			
			return hook_fetchLSN(lsn, in,inEntry);
		}

		private Node hook_fetchLSN(long lsn, IN in, INEntry inEntry) throws DatabaseException {
			int index = inEntry.index;
			if (in.isEntryKnownDeleted(index) || in.getLsn(index) != lsn) {
				return null;
			}
			return in.fetchTarget(index);
		}
	}

	/**
	 * Preload the cache, using up to maxBytes bytes or maxMillsecs msec.
	 */
	public PreloadResult preload(PreloadConfig config) throws DatabaseException {

		long maxMillisecs = config.getMaxMillisecs();
		long targetTime = Long.MAX_VALUE;
		if (maxMillisecs > 0) {
			targetTime = System.currentTimeMillis() + maxMillisecs;
		}



		PreloadResult ret = new PreloadResult();

		PreloadProcessor callback = new PreloadProcessor(envImpl, /*//M.B maxBytes,*/
				targetTime,ret);
		SortedLSNTreeWalker walker = new PreloadLSNTreeWalker(this, callback,
				config);
		
		try {
			walker.walk();
		} catch (HaltPreloadException HPE) {
			ret.setStatus(HPE.getStatus());
		}

		return ret;
	}

	/*
	 * Dumping
	 */
	public String dumpString(int nSpaces) {
		StringBuffer sb = new StringBuffer();
		sb.append(TreeUtils.indent(nSpaces));
		sb.append("<database id=\"");
		sb.append(id.toString());
		sb.append("\"");
		if (btreeComparator != null) {
			sb.append(" btc=\"");
			sb.append(serializeComparator(btreeComparator));
			sb.append("\"");
		}
		if (duplicateComparator != null) {
			sb.append(" dupc=\"");
			sb.append(serializeComparator(duplicateComparator));
			sb.append("\"");
		}
		sb.append("/>");
		return sb.toString();
	}

	/*
	 * Logging support
	 */

	/**
	 * @see LogWritable#getLogSize
	 */
	public int getLogSize() {
		return id.getLogSize()
				+ tree.getLogSize()
				+ LogUtils.getBooleanLogSize()
				+ LogUtils
						.getStringLogSize(serializeComparator(btreeComparator))
				+ LogUtils
						.getStringLogSize(serializeComparator(duplicateComparator))
				+ (LogUtils.getIntLogSize() * 2);
	}

	/**
	 * @see LogWritable#writeToLog
	 */
	public void writeToLog(ByteBuffer logBuffer) {
		id.writeToLog(logBuffer);
		tree.writeToLog(logBuffer);
		LogUtils.writeBoolean(logBuffer, duplicatesAllowed);
		LogUtils.writeString(logBuffer, serializeComparator(btreeComparator));
		LogUtils.writeString(logBuffer,
				serializeComparator(duplicateComparator));
		LogUtils.writeInt(logBuffer, maxMainTreeEntriesPerNode);
		LogUtils.writeInt(logBuffer, maxDupTreeEntriesPerNode);
	}

	/**
	 * @see LogReadable#readFromLog
	 */
	public void readFromLog(ByteBuffer itemBuffer, byte entryTypeVersion)
			throws LogException {

		id.readFromLog(itemBuffer, entryTypeVersion);
		tree.readFromLog(itemBuffer, entryTypeVersion);
		duplicatesAllowed = LogUtils.readBoolean(itemBuffer);

		btreeComparatorName = LogUtils.readString(itemBuffer);
		duplicateComparatorName = LogUtils.readString(itemBuffer);

		try {
			if (!EnvironmentImpl.getNoComparators()) {

				/*
				 * Don't instantiate if comparators are unnecessary
				 * (DbPrintLog).
				 */
				if (btreeComparatorName.length() != 0) {
					Class btreeComparatorClass = Class
							.forName(btreeComparatorName);
					btreeComparator = instantiateComparator(
							btreeComparatorClass, "Btree");
				}
				if (duplicateComparatorName.length() != 0) {
					Class duplicateComparatorClass = Class
							.forName(duplicateComparatorName);
					duplicateComparator = instantiateComparator(
							duplicateComparatorClass, "Duplicate");
				}
			}
		} catch (ClassNotFoundException CNFE) {
			throw new LogException("couldn't instantiate class comparator",
					CNFE);
		}

		if (entryTypeVersion >= 1) {
			maxMainTreeEntriesPerNode = LogUtils.readInt(itemBuffer);
			maxDupTreeEntriesPerNode = LogUtils.readInt(itemBuffer);
		}
	}

	/**
	 * @see LogReadable#dumpLog
	 */
	public void dumpLog(StringBuffer sb, boolean verbose) {
		sb.append("<database>");
		id.dumpLog(sb, verbose);
		tree.dumpLog(sb, verbose);
		sb.append("<dupsort v=\"").append(duplicatesAllowed);
		sb.append("\"/>");
		sb.append("<btcf name=\"");
		sb.append(btreeComparatorName);
		sb.append("\"/>");
		sb.append("<dupcf name=\"");
		sb.append(duplicateComparatorName);
		sb.append("\"/>");
		sb.append("</database>");
	}

	/**
	 * @see LogReadable#logEntryIsTransactional
	 */
	public boolean logEntryIsTransactional() {
		return false;
	}

	/**
	 * @see LogReadable#getTransactionId
	 */
	public long getTransactionId() {
		return 0;
	}

	/**
	 * Used both to write to the log and to validate a comparator when set in
	 * DatabaseConfig.
	 */
	public static String serializeComparator(Comparator comparator) {
		if (comparator != null) {
			return comparator.getClass().getName();
		} else {
			return "";
		}
	}

	/**
	 * Used both to read from the log and to validate a comparator when set in
	 * DatabaseConfig.
	 */
	public static Comparator instantiateComparator(Class comparator,
			String comparatorType) throws LogException {

		if (comparator == null) {
			return null;
		}

		try {
			return (Comparator) comparator.newInstance();
		} catch (InstantiationException IE) {
			throw new LogException("Exception while trying to load "
					+ comparatorType + " Comparator class: " + IE);
		} catch (IllegalAccessException IAE) {
			throw new LogException("Exception while trying to load "
					+ comparatorType + " Comparator class: " + IAE);
		}
	}

	public int getBinDeltaPercent() {
		return binDeltaPercent;
	}

	public int getBinMaxDeltas() {
		return binMaxDeltas;
	}
	
	/**
	 * Return the count of nodes in the database. Used for truncate, perhaps
	 * should be made available through other means? Database should be
	 * quiescent.
	 */
	long countRecords() throws DatabaseException {

		LNCounter lnCounter = new LNCounter();

		/* future enchancement -- use a walker that does not fetch dup trees */
		SortedLSNTreeWalker walker = new SortedLSNTreeWalker(this, false, // don't
				// remove
				// from
				// INList
				tree.getRootLsn(), lnCounter);

		walker.walk();
		return lnCounter.getCount();
	}
}
