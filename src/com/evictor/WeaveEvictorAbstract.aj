package com.evictor;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.cleaner.Cleaner;
import com.sleepycat.je.cleaner.UtilizationProfile;
import com.sleepycat.je.dbi.CursorImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.recovery.Checkpointer;
import com.sleepycat.je.recovery.RecoveryManager;
import com.sleepycat.je.recovery.Checkpointer.CheckpointStartResult;
import com.sleepycat.je.tree.LN;
import com.sleepycat.je.tree.Node;
import com.sleepycat.je.Environment;
import com.sleepycat.je.cleaner.UtilizationTracker;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.IN;

public privileged abstract aspect WeaveEvictorAbstract {

	pointcut requestShutdownDaemons(EnvironmentImpl env) 
	 : execution(void EnvironmentImpl.requestShutdownDaemons()) && this(env);

	pointcut hook_checkpointStart(Checkpointer cp) 
	 : execution(CheckpointStartResult Checkpointer.hook_checkpointStart(String, boolean, boolean)) && 
		this(cp);

	pointcut rebuildINListOrhook_invokeEvictor(RecoveryManager recoveryManager) 
	 : ((call(void RecoveryManager.rebuildINList()) && 
		withincode(void RecoveryManager.buildTree())) ||
		(call(void hook_invokeEvictor()) && 
		within(RecoveryManager))
		) && this(recoveryManager);

	pointcut hook_evictCursor(CursorImpl cursor) 
	 : execution(void UtilizationProfile.hook_evictCursor(CursorImpl)) && args(cursor);
	
	after(EnvironmentImpl env):
		requestShutdownDaemons(env){
		if (env.evictor != null) {
			env.evictor.requestShutdown();
		}
	}

	CheckpointStartResult around(Checkpointer cp):
		hook_checkpointStart(cp) {
		synchronized (cp.envImpl.getEvictor()) {
			return proceed(cp);
		}
	}

	// TODO could have been a nice homogenic crosscutting advice (though only
	// two pieces of code, but still)
	// but not beeing able to throw a new exception forces hook method.
	after(RecoveryManager recoveryManager) throws DatabaseException :
		rebuildINListOrhook_invokeEvictor(recoveryManager) {
		recoveryManager.env.invokeEvictor();
	}

	after(CursorImpl cursor) throws DatabaseException :
		execution(void UtilizationProfile.hook_evictCursor(CursorImpl)) && args(cursor) {
		cursor.evict();
	}

	/* Daemons */
	private Evictor EnvironmentImpl.evictor;

	public void EnvironmentImpl.invokeEvictor() throws DatabaseException {

		if (evictor != null) {
			evictor.doEvict(Evictor.SOURCE_MANUAL);
		}
	}

	public Evictor EnvironmentImpl.getEvictor() {
		return evictor;
	}

	/**
	 * Javadoc for this public method is generated via the doc templates in the
	 * doc_src directory.
	 */
	public void Environment.evictMemory() throws DatabaseException {

		checkHandleIsValid();
		checkEnv();
		environmentImpl.invokeEvictor();
	}

	/**
	 * Reduce memory consumption by evicting all LN targets. Note that the
	 * targets are not persistent, so this doesn't affect node dirtiness.
	 * 
	 * The BIN should be latched by the caller.
	 * 
	 * @return number of evicted bytes
	 */
	public long BIN.evictLNs() throws DatabaseException {

		Cleaner cleaner = getDatabase().getDbEnvironment().getCleaner();

		/*
		 * We can't evict an LN which is pointed to by a cursor, in case that
		 * cursor has a reference to the LN object. We'll take the cheap choice
		 * and avoid evicting any LNs if there are cursors on this BIN. We could
		 * do a more expensive, precise check to see entries have which cursors.
		 * (We'd have to be careful to use the right field, index vs dupIndex).
		 * This is something we might move to later.
		 */
		long removed = 0;
		if (nCursors() == 0) {
			for (int i = 0; i < getNEntries(); i++) {
				removed += evictInternal(i, cleaner);
			}
		}
		return removed;
	}

	/**
	 * Evict a single LN if allowed and adjust the memory budget.
	 */
	public void BIN.evictLN(int index) throws DatabaseException {

		Cleaner cleaner = getDatabase().getDbEnvironment().getCleaner();
		evictInternal(index, cleaner);
	}

	/**
	 * Evict a single LN if allowed. The amount of memory freed is returned and
	 * must be subtracted from the memory budget by the caller.
	 */
	private long BIN.evictInternal(int index, Cleaner cleaner)
			throws DatabaseException {

		Node n = getTarget(index);

		/* Don't strip LNs that the cleaner will be migrating. */
		if (n instanceof LN && cleaner.isEvictable(this, index)) {
			setTarget(index, null);
			return n.getMemorySizeIncludedByParent();
		} else {
			return 0;
		}
	}

	/**
	 * Returns whether this node can itself be evicted. This is faster than
	 * (getEvictionType() == MAY_EVICT_NODE) and is used by the evictor after a
	 * node has been selected, to check that it is still evictable.
	 */
	public boolean IN.isEvictable() {

		if (isEvictionProhibited()) {
			return false;
		}

		if (hasNonLNChildren()) {
			return false;
		}

		return true;
	}

	/**
	 * Returns the eviction type for this IN, for use by the evictor. Uses the
	 * internal isEvictionProhibited and getChildEvictionType methods that may
	 * be overridden by subclasses.
	 * 
	 * @return MAY_EVICT_LNS if evictable LNs may be stripped; otherwise,
	 *         MAY_EVICT_NODE if the node itself may be evicted; otherwise,
	 *         MAY_NOT_EVICT.
	 */
	public int IN.getEvictionType() {

		if (isEvictionProhibited()) {
			return MAY_NOT_EVICT;
		} else {
			return getChildEvictionType();
		}
	}

	/**
	 * Returns the eviction type based on the status of child nodes,
	 * irrespective of isEvictionProhibited.
	 */
	int IN.getChildEvictionType() {

		return hasResidentChildren() ? MAY_NOT_EVICT : MAY_EVICT_NODE;
	}

	/**
	 * Returns whether the node is not evictable, irrespective of the status of
	 * the children nodes.
	 */
	boolean IN.isEvictionProhibited() {

		return isDbRoot();
	}

	/**
	 * Evict the LN node at the cursor position. This is used for internal
	 * databases only.
	 */
	public void CursorImpl.evict() throws DatabaseException {
		setTargetBin();
		targetBin.evictLN(targetIndex);
	}

	/**
	 * Evicts tracked detail if the budget for the tracker is exceeded. Evicts
	 * only one file summary LN at most to keep eviction batches small. Returns
	 * the number of bytes freed.
	 * 
	 * <p>
	 * When flushFileSummary is called, the TrackedFileSummary is cleared via
	 * its reset method, which is called by FileSummaryLN.writeToLog. This is
	 * how memory is subtracted from the budget.
	 * </p>
	 */
	public long UtilizationTracker.evictMemory() throws DatabaseException {

		return 0;
	}
}
