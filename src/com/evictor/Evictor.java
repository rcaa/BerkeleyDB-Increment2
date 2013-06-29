/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002-2006
 *      Sleepycat Software.  All rights reserved.
 *
 * $Id: Evictor.java,v 1.1.2.5 2007/03/01 23:15:18 ckaestne Exp $
 */

package com.evictor;


import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.INList;
import com.sleepycat.je.log.LogManager;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.tree.Node;
import com.sleepycat.je.tree.SearchResult;
import com.sleepycat.je.tree.Tree;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.TestHook;

/**
 * The Evictor looks through the INList for IN's and BIN's that are worthy of
 * eviction. Once the nodes are selected, it removes all references to them so
 * that they can be GC'd by the JVM.
 */
//TODO evictor requires memorybudget
public class Evictor {

	public static final String SOURCE_MANUAL = "manual";

	public static final String SOURCE_CRITICAL = "critical";

	private static final boolean DEBUG = false;

	private EnvironmentImpl envImpl;

	private LogManager logManager;

//	private Level detailedTraceLevel; // level value for detailed trace msgs

	private volatile boolean active; // true if eviction is happening.

	/* Round robin marker in the INList, indicates start of eviction scans. */
	private IN nextNode;

	/* The number of bytes we need to evict in order to get under budget. */
	private long currentRequiredEvictBytes;

	/* 1 node out of <nodesPerScan> are chosen for eviction. */
	private int nodesPerScan;

	/* je.evictor.evictBytes */
	private long evictBytesSetting;

	/* je.evictor.lruOnly */
	private boolean evictByLruOnly;

	/* for trace messages. */
	private NumberFormat formatter;


	private int nNodesScannedThisRun;

	/* Debugging and unit test support. */
	EvictProfile evictProfile;

	private TestHook runnableHook;

	private String name;

	private boolean shutdownRequest=false;

	public Evictor(EnvironmentImpl envImpl, String name)
			throws DatabaseException {

		this.envImpl = envImpl;
		logManager = envImpl.getLogManager();
		nextNode = null;

		DbConfigManager configManager = envImpl.getConfigManager();
		nodesPerScan = configManager
				.getInt(EnvironmentParams.EVICTOR_NODES_PER_SCAN);
		evictBytesSetting = configManager
				.getLong(EnvironmentParams.EVICTOR_EVICT_BYTES);
		evictByLruOnly = configManager
				.getBoolean(EnvironmentParams.EVICTOR_LRU_ONLY);
//TODO		detailedTraceLevel = Tracer.parseLevel(envImpl,
//				EnvironmentParams.JE_LOGGING_LEVEL_EVICTOR);

		evictProfile = new EvictProfile();
		formatter = NumberFormat.getNumberInstance();

		active = false;
		this.name=name;
	}

	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("<Evictor name=\"").append(name).append("\"/>");
		return sb.toString();
	}




	synchronized public void clearEnv() {
		envImpl = null;
	}

	public synchronized boolean isActive(){
		return active;
	}





	/**
	 * May be called by the evictor thread on wakeup or programatically.
	 */
	public void doEvict(String source) throws DatabaseException {

		doEvict(source, false /* evictDuringShutdown */);
	}

	/**
	 * Allows performing eviction during shutdown, which is needed when during
	 * checkpointing and cleaner log file deletion.
	 */
	private synchronized void doEvict(String source, boolean evictDuringShutdown)
			throws DatabaseException {

		/*
		 * We use an active flag to prevent reentrant calls. This is simpler
		 * than ensuring that no reentrant eviction can occur in any caller. We
		 * also use the active flag to determine when it is unnecessary to wake
		 * up the evictor thread.
		 */
		if (active) {
			return;
		}
		active = true;
		try {

			/*
			 * Repeat as necessary to keep up with allocations. Stop if no
			 * progress is made, to prevent an infinite loop.
			 */
			boolean progress = true;
			while (progress && (evictDuringShutdown || !shutdownRequest)
					&& isRunnable(source)) {
				if (evictBatch(source, currentRequiredEvictBytes) == 0) {
					progress = false;
				}
			}
		} finally {
			active = false;
		}
	}



	/**
	 * Each iteration will latch and unlatch the major INList, and will attempt
	 * to evict requiredEvictBytes, but will give up after a complete pass over
	 * the major INList. Releasing the latch is important because it provides an
	 * opportunity for to add the minor INList to the major INList.
	 * 
	 * @return the number of bytes evicted, or zero if no progress was made.
	 */
	long evictBatch(String source, long requiredEvictBytes)
			throws DatabaseException {



		assert evictProfile.clear(); // intentional side effect
		long evictBytes = 0;

		/* Evict utilization tracking info without holding the INList latch. */
		evictBytes += envImpl.getUtilizationTracker().evictMemory();

		INList inList = envImpl.getInMemoryINs();
		
		evictBytes += hook_evictInternal(inList,requiredEvictBytes);
		
		return evictBytes;
	}

	private long hook_evictInternal(INList inList, long requiredEvictBytes) throws DatabaseException {
		long evictBytes=0;
		int nBatchSets = 0;
		boolean finished = false;

		int inListStartSize = inList.getSize();

		try {

			/*
			 * Setup the round robin iterator. Note that because critical
			 * eviction is now called during recovery, when the INList is
			 * sometimes abruptly cleared, nextNode may not be null when the
			 * INList is empty.
			 */
			if (inListStartSize == 0) {
				nextNode = null;
				return 0;
			} else {
				if (nextNode == null) {
					nextNode = inList.first();
				}
			}

			ScanIterator scanIter = new ScanIterator(nextNode, inList);

			/*
			 * Keep evicting until we've freed enough memory or we've visited
			 * the maximum number of nodes allowed. Each iteration of the while
			 * loop is called an eviction batch.
			 * 
			 * In order to prevent endless evicting and not keep the INList
			 * major latch for too long, limit this run to one pass over the IN
			 * list.
			 */
			while ((evictBytes < requiredEvictBytes)
					&& (nNodesScannedThisRun <= inListStartSize)) {

				IN target = selectIN(inList, scanIter);

				if (target == null) {
					break;
				} else {
					assert evictProfile.count(target);// intentional side
														// effect
					evictBytes += evict(inList, target, scanIter);
				}
				nBatchSets++;
			}

			/*
			 * At the end of the scan, look at the next element in the INList
			 * and put it in nextNode for the next time we scan the INList.
			 */
			nextNode = scanIter.mark();
			finished = true;

		} finally {
			

			// Logger logger = envImpl.getLogger();
			// if (logger.isLoggable(detailedTraceLevel)) {
			/* Ugh, only create trace message when logging. */
			// refined trace Tracer.trace(detailedTraceLevel, envImpl,
			/*
			 * "Evictor: pass=" + nEvictPasses + " finished=" + finished + "
			 * source=" + source + " requiredEvictBytes=" +
			 * formatter.format(requiredEvictBytes) + " evictBytes=" +
			 * formatter.format(evictBytes) + " inListSize=" + inListStartSize + "
			 * nNodesScanned=" + nNodesScannedThisRun + " nNodesSelected=" +
			 * nNodesSelectedThisRun + " nEvicted=" + nNodesEvictedThisRun + "
			 * nBINsStripped=" + nBINsStrippedThisRun + " nBatchSets=" +
			 * nBatchSets);
			 */
			// }
		}
		return evictBytes;
	}

	/**
	 * Return true if eviction should happen.
	 */
	boolean isRunnable(String source) throws DatabaseException {
		return false;
	}

	/**
	 * Select a single node to evict.
	 */
	private IN selectIN(INList inList, ScanIterator scanIter)
			throws DatabaseException {

		/* Find the best target in the next <nodesPerScan> nodes. */
		IN target = null;
		long targetGeneration = Long.MAX_VALUE;
		int targetLevel = Integer.MAX_VALUE;
		boolean targetDirty = true;
		boolean envIsReadOnly = envImpl.isReadOnly();
		int scanned = 0;
		boolean wrapped = false;
		while (scanned < nodesPerScan) {
			if (scanIter.hasNext()) {
				IN in = scanIter.next();
				nNodesScannedThisRun++;

				DatabaseImpl db = in.getDatabase();

				if (hook_checkDeleted(db,in))
					continue;

				/*
				 * Don't evict the DatabaseImpl Id Mapping Tree (db 0), both for
				 * object identity reasons and because the id mapping tree
				 * should stay cached.
				 */
				if (db.getId().equals(DbTree.ID_DB_ID)) {
					continue;
				}

				/*
				 * If this is a read only database and we have at least one
				 * target, skip any dirty INs (recovery dirties INs even in a
				 * read-only environment). We take at least one target so we
				 * don't loop endlessly if everything is dirty.
				 */
				if (envIsReadOnly && (target != null) && in.getDirty()) {
					continue;
				}

				/*
				 * Only scan evictable or strippable INs. This prevents higher
				 * level INs from being selected for eviction, unless they are
				 * part of an unused tree.
				 */
				int evictType = in.getEvictionType();
				if (evictType == IN.MAY_NOT_EVICT) {
					continue;
				}

				/*
				 * This node is in the scanned node set. Select according to the
				 * configured eviction policy.
				 */
				if (evictByLruOnly) {

					/*
					 * Select the node with the lowest generation number,
					 * irrespective of tree level or dirtyness.
					 */
					if (targetGeneration > in.getGeneration()) {
						targetGeneration = in.getGeneration();
						target = in;
					}
				} else {

					/*
					 * Select first by tree level, then by dirtyness, then by
					 * generation/LRU.
					 */
					int level = normalizeLevel(in, evictType);
					if (targetLevel != level) {
						if (targetLevel > level) {
							targetLevel = level;
							targetDirty = in.getDirty();
							targetGeneration = in.getGeneration();
							target = in;
						}
					} else if (targetDirty != in.getDirty()) {
						if (targetDirty) {
							targetDirty = false;
							targetGeneration = in.getGeneration();
							target = in;
						}
					} else {
						if (targetGeneration > in.getGeneration()) {
							targetGeneration = in.getGeneration();
							target = in;
						}
					}
				}
				scanned++;
			} else {
				/* We wrapped around in the list. */
				if (wrapped) {
					break;
				} else {
					nextNode = inList.first();
					scanIter.reset(nextNode);
					wrapped = true;
				}
			}
		}

		
		return target;
	}

	private boolean hook_checkDeleted(DatabaseImpl db, IN in) throws DatabaseException{
		return false;
	}

	/**
	 * Normalize the tree level of the given IN.
	 * 
	 * Is public for unit testing.
	 * 
	 * A BIN containing evictable LNs is given level 0, so it will be stripped
	 * first. For non-duplicate and DBMAP trees, the high order bits are cleared
	 * to make their levels correspond; that way, all bottom level nodes (BINs
	 * and DBINs) are given the same eviction priority.
	 * 
	 * Note that BINs in a duplicate tree are assigned the same level as BINs in
	 * a non-duplicate tree. This isn't always optimimal, but is the best we can
	 * do considering that BINs in duplicate trees may contain a mix of LNs and
	 * DINs.
	 */
	public int normalizeLevel(IN in, int evictType) {

		int level = in.getLevel() & IN.LEVEL_MASK;

		if (level == 1 && evictType == IN.MAY_EVICT_LNS) {
			level = 0;
		}

		return level;
	}

	/**
	 * Strip or evict this node.
	 * 
	 * @return number of bytes evicted.
	 */
	private long evict(INList inList, IN target, ScanIterator scanIter)
			throws DatabaseException {

		boolean envIsReadOnly = envImpl.isReadOnly();
		long evictedBytes = 0;

		/*
		 * Non-BIN INs are evicted by detaching them from their parent. For
		 * BINS, the first step is to remove deleted entries by compressing the
		 * BIN. The evictor indicates that we shouldn't fault in non-resident
		 * children during compression. After compression, LN stripping may be
		 * performed.
		 * 
		 * If LN stripping is used, first we strip the BIN by merely detaching
		 * all its resident LN targets. If we make progress doing that, we stop
		 * and will not evict the BIN itself until possibly later. If it has no
		 * resident LNs then we evict the BIN itself using the "regular"
		 * detach-from-parent routine.
		 * 
		 * If the cleaner is doing clustering, we don't do BIN stripping if we
		 * can write out the BIN. Specifically LN stripping is not performed if
		 * the BIN is dirty AND the BIN is evictable AND cleaner clustering is
		 * enabled. In this case the BIN is going to be written out soon, and
		 * with clustering we want to be sure to write out the LNs with the BIN;
		 * therefore we don't do stripping
		 */

		/*
		 * Use latchNoWait because if it's latched we don't want the cleaner to
		 * hold up eviction while it migrates an entire BIN. Latched INs have a
		 * high generation value, so not evicting makes sense. Pass false
		 * because we don't want to change the generation during the eviction
		 * process.
		 */
				if (target instanceof BIN) {
					/* first attempt to compress deleted, resident children. */
					envImpl.lazyCompress(target);

					/*
					 * Strip any resident LN targets right now. No need to dirty
					 * the BIN, the targets are not persistent data.
					 */
					evictedBytes = ((BIN) target).evictLNs();
					
				}

				/*
				 * If we were able to free any memory by LN stripping above,
				 * then we postpone eviction of the BIN until a later pass.
				 */
				if (evictedBytes == 0 && target.isEvictable()) {
					/* Regular eviction. */
					Tree tree = target.getDatabase().getTree();

					/* getParentINForChildIN unlatches target. */
					SearchResult result = tree.getParentINForChildIN(target,
							true, // requireExactMatch
							false); // updateGeneration
					if (result.exactParentFound) {
						evictedBytes = evictIN(target, result.parent,
								result.index, inList, scanIter, envIsReadOnly);
					}
				}


		return evictedBytes;
	}

	/**
	 * Evict an IN. Dirty nodes are logged before they're evicted. inlist is
	 * latched with the major latch by the caller.
	 */
	private long evictIN(IN child, IN parent, int index, INList inlist,
			ScanIterator scanIter, boolean envIsReadOnly)
			throws DatabaseException {

		long evictBytes = 0;

			long oldGenerationCount = child.getGeneration();

			/*
			 * Get a new reference to the child, in case the reference saved in
			 * the selection list became out of date because of changes to that
			 * parent.
			 */
			IN renewedChild = (IN) parent.getTarget(index);

			/*
			 * See the evict() method in this class for an explanation for
			 * calling latchNoWait(false).
			 */
			if ((renewedChild != null)
					&& (renewedChild.getGeneration() <= oldGenerationCount)
					&& renewedChild.hook_latchNoWait(false)
					) {

				try {
					if (renewedChild.isEvictable()) {

						/*
						 * Log the child if dirty and env is not r/o. Remove
						 * from IN list.
						 */
						long renewedChildLsn = DbLsn.NULL_LSN;
						boolean newChildLsn = false;
						if (renewedChild.getDirty()) {
							if (!envIsReadOnly) {

								/*
								 * Determine whether provisional logging is
								 * needed. The checkpointer can be null if it
								 * was shutdown or never started.
								 */
								boolean logProvisional = (envImpl
										.getCheckpointer() != null && (renewedChild
										.getLevel() < envImpl.getCheckpointer()
										.getHighestFlushLevel()));

								/*
								 * Log a full version (no deltas) and with
								 * cleaner migration allowed.
								 */
								renewedChildLsn = renewedChild.log(logManager,
										false, // allowDeltas
										logProvisional, true, // proactiveMigration
										parent);
								newChildLsn = true;
							}
						} else {
							renewedChildLsn = parent.getLsn(index);
						}

						if (renewedChildLsn != DbLsn.NULL_LSN) {
							/* Take this off the inlist. */
							scanIter.mark();
							inlist.remove/*LatchAlreadyHeld*/(renewedChild);
							scanIter.resetToMark();

							evictBytes = hook_getSize(renewedChild);
							if (newChildLsn) {

								/*
								 * Update the parent so its reference is null
								 * and it has the proper LSN.
								 */
								parent
										.updateEntry(index, null,
												renewedChildLsn);
							} else {

								/*
								 * Null out the reference, but don't dirty the
								 * node since only the reference changed.
								 */
								parent.updateEntry(index, (Node) null);
							}

							hook_evictedIN();
						}
					}
				} finally {
					renewedChild.hook_releaseLatch();
				}
			}


		return evictBytes;
	}

	private long hook_getSize(IN renewedChild) {
		return 0;
	}

	private void hook_evictedIN() {
		// TODO Auto-generated method stub
		
	}

	/**
	 * Used by unit tests.
	 */
	IN getNextNode() {
		return nextNode;
	}

	/* For unit testing only. */
	public void setRunnableHook(TestHook hook) {
		runnableHook = hook;
	}

	/* For debugging and unit tests. */
	static public class EvictProfile {
		/* Keep a list of candidate nodes. */
		private List candidates = new ArrayList();

		/* Remember that this node was targetted. */
		public boolean count(IN target) {
			candidates.add(new Long(target.getNodeId()));
			return true;
		}

		public List getCandidates() {
			return candidates;
		}

		public boolean clear() {
			candidates.clear();
			return true;
		}
	}

	/*
	 * ScanIterator keeps a handle onto the current round robin INList iterator.
	 * It's deliberately not a member of the class in order to keep less common
	 * state in the class.
	 */
	public static class ScanIterator {
		private INList inList;

		private Iterator iter;

		private IN nextMark;

		ScanIterator(IN startingIN, INList inList) throws DatabaseException {

			this.inList = inList;
			reset(startingIN);
		}

		void reset(IN startingIN) throws DatabaseException {

			iter = inList.tailSet(startingIN).iterator();
		}

		IN mark() throws DatabaseException {

			if (iter.hasNext()) {
				nextMark = (IN) iter.next();
			} else {
				nextMark = (IN) inList.first();
			}
			return (IN) nextMark;
		}

		void resetToMark() throws DatabaseException {

			reset(nextMark);
		}

		boolean hasNext() {
			return iter.hasNext();
		}

		IN next() {
			return (IN) iter.next();
		}

		void remove() {
			iter.remove();
		}
	}
	
	public void requestShutdown() {
		shutdownRequest = true;
	}
}
