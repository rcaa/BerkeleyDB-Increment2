package com.memorybudget;

import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.tree.ChildReference;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.tree.Node;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.INList;
import com.sleepycat.je.log.LogManager;

public privileged aspect INBudgetAbstract {

	pointcut postRecoveryInit(IN in) : (execution(IN.new(DatabaseImpl, byte[], int, int))  ||
			execution(void IN.postRecoveryInit(DatabaseImpl, long))) && this(in);

	pointcut addOrRebuildINList(IN in) : ((call(void INList.add(IN)) && withincode(void IN.postFetchInit(DatabaseImpl, long))) ||
		execution(void IN.rebuildINList(INList))) && this(in);

	pointcut monitorEntryMemorySizeChanges(int idx) :
		execution(void IN.updateEntry*(int,Node,..))
		&& args(idx,..);

	pointcut setEntry(IN in, int idx) : execution(void IN.setEntry(int,Node,byte[],long,byte)) &&
		args(idx,Node,byte[],long,byte) && this(in);

	pointcut adjustCursorsForInsert(IN in, int index) : call(void adjustCursorsForInsert(int)) && args(index) && 
		withincode(int IN.insertEntry1(ChildReference)) && this(in);

	pointcut hookr_deleteEntryInternal(IN in, int index) : execution(void IN.hookr_deleteEntryInternal(int)) && args(index) && this(in);

	pointcut monitorOverheadSizeChanges() :
		execution(void IN.setLsn(int,long)) ||
		call(void IN.shiftEntriesRight(int)) || 
		execution(void IN.hookr_deleteEntryInternal(int));

	pointcut hookr_splitInternal_work2(IN in) : execution(void IN.hookr_splitInternal_work2(..)) && this(in);

	pointcut trackProvisionalObsolete(IN in, IN child, long obsoleteLsn1,
			long obsoleteLsn2) 
	: execution(void IN.trackProvisionalObsolete(IN,long,long)) 
	&& args(child,obsoleteLsn1,obsoleteLsn2) && this(in);

	pointcut flushProvisionalObsolete(IN in) 
	: execution(void IN.flushProvisionalObsolete(LogManager))  && this(in);

	pointcut fetchTarget2(IN in) : execution(Node IN.fetchTarget(int)) && this(in);

	after(IN in) : postRecoveryInit(in) {
		in.initMemorySize();
	}

	before(IN in) : addOrRebuildINList(in) {
		in.initMemorySize();
	}

	void around(IN in) :  monitorOverheadSizeChanges() && this(in){
		int oldSize = in.computeLsnOverhead();// MB
		proceed(in);
		in.changeMemorySize(in.computeLsnOverhead() - oldSize);// MB
	}

	void around(IN in, int idx) :  monitorEntryMemorySizeChanges(idx) && this(in){
		long oldSize = in.getEntryInMemorySize(idx);// MB
		proceed(in, idx);
		long newSize = in.getEntryInMemorySize(idx);
		in.updateMemorySize(oldSize, newSize);// MB
	}

	// TODO not homogenic with the one before because contains special if
	void around(IN in, int idx) : setEntry(in, idx) {
		long oldSize;
		if (idx + 1 > in.nEntries)
			oldSize = 0;
		else
			oldSize = in.getEntryInMemorySize(idx);// MB

		proceed(in, idx);
		long newSize = in.getEntryInMemorySize(idx);
		in.updateMemorySize(oldSize, newSize);// MB
	}

	after(IN in, int index):
		adjustCursorsForInsert(in, index) {
		in.updateMemorySize(0, in.getEntryInMemorySize(index));// MB
	}

	before(IN in, int index) : hookr_deleteEntryInternal(in, index) {
		in.updateMemorySize(in.getEntryInMemorySize(index), 0);// MB
	}

	void around(IN in) : hookr_splitInternal_work2(in) {
		long oldMemorySize = in.inMemorySize;// MB
		proceed(in);
		long newSize = in.computeMemorySize();// MB
		in.updateMemorySize(oldMemorySize, newSize);
	}

	void around(IN in, IN child, long obsoleteLsn1, long obsoleteLsn2) :
		trackProvisionalObsolete(in, child, obsoleteLsn1, obsoleteLsn2) {
		int memDelta = 0;// MB

		if (child.provisionalObsolete != null) {

			int childMemDelta = child.provisionalObsolete.size()
					* MemoryBudget.LONG_LIST_PER_ITEM_OVERHEAD;// MB
			child.changeMemorySize(0 - childMemDelta);// MB
			memDelta += childMemDelta;
		}

		if (obsoleteLsn1 != DbLsn.NULL_LSN) {
			memDelta += MemoryBudget.LONG_LIST_PER_ITEM_OVERHEAD;// MB
		}

		if (obsoleteLsn2 != DbLsn.NULL_LSN) {
			memDelta += MemoryBudget.LONG_LIST_PER_ITEM_OVERHEAD;// MB
		}

		proceed(in, child, obsoleteLsn1, obsoleteLsn2);

		if (memDelta != 0) {
			in.changeMemorySize(memDelta);// MB
		}
	}

	void around(IN in) : flushProvisionalObsolete (in) {
		int memDelta = 0;
		if (in.provisionalObsolete != null) {
			memDelta = in.provisionalObsolete.size()
					* MemoryBudget.LONG_LIST_PER_ITEM_OVERHEAD;
		}
		proceed(in);
		if (memDelta != 0) {
			in.changeMemorySize(0 - memDelta);// MB
		}
	}

	after(IN in) returning(Node resultNode):
		fetchTarget2(in) {
		in.updateMemorySize(null, resultNode);
	}

	/*
	 * accumluted memory budget delta. Once this exceeds
	 * MemoryBudget.ACCUMULATED_LIMIT we inform the MemoryBudget that a change
	 * has occurred. See SR 12273.
	 */
	private int IN.accumulatedDelta = 0;// MB

	private long IN.inMemorySize;// MB

	/**
	 * Initialize the per-node memory count by computing its memory usage.
	 */
	void IN.initMemorySize() {// MB
		inMemorySize = computeMemorySize();
	}

	/*
	 * Memory usage calculations.
	 */
	public boolean IN.verifyMemorySize() {// MB

		long calcMemorySize = computeMemorySize();
		if (calcMemorySize != inMemorySize) {

			String msg = "-Warning: Out of sync. " + "Should be "
					+ calcMemorySize + " / actual: " + inMemorySize + " node: "
					+ getNodeId();
			// refined trace Tracer.trace(Level.INFO,
			// databaseImpl.getDbEnvironment(), msg);

			System.out.println(msg);

			return false;
		} else {
			return true;
		}
	}

	/**
	 * Return the number of bytes used by this IN. Latching is up to the caller.
	 */
	public long IN.getInMemorySize() {// MB
		return inMemorySize;
	}

	private long IN.getEntryInMemorySize(int idx) {// MB
		return getEntryInMemorySize(entryKeyVals[idx], entryTargets[idx]);
	}

	long IN.getEntryInMemorySize(byte[] key, Node target) {

		/*
		 * Do not count state size here, since it is counted as overhead during
		 * initialization.
		 */
		long ret = 0;
		if (key != null) {
			ret += MemoryBudget.byteArraySize(key.length);
		}
		if (target != null) {
			ret += target.getMemorySizeIncludedByParent();
		}
		return ret;
	}

	/**
	 * Count up the memory usage attributable to this node alone. LNs children
	 * are counted by their BIN/DIN parents, but INs are not counted by their
	 * parents because they are resident on the IN list.
	 */
	long IN.computeMemorySize() {// MB
		MemoryBudget mb = databaseImpl.getDbEnvironment().getMemoryBudget();
		long calcMemorySize = getMemoryOverhead(mb);
		calcMemorySize += computeLsnOverhead();
		for (int i = 0; i < nEntries; i++) {
			calcMemorySize += getEntryInMemorySize(i);
		}
		/*
		 * XXX Need to update size when changing the identifierKey. if
		 * (identifierKey != null) { calcMemorySize +=
		 * MemoryBudget.byteArraySize(identifierKey.length); }
		 */

		if (provisionalObsolete != null) {
			calcMemorySize += provisionalObsolete.size()
					* MemoryBudget.LONG_LIST_PER_ITEM_OVERHEAD;
		}

		return calcMemorySize;
	}

	/* Called once at environment startup by MemoryBudget. */
	public static long IN.computeOverhead(DbConfigManager configManager)
			throws DatabaseException {// MB

		/*
		 * Overhead consists of all the fields in this class plus the entry
		 * arrays in the IN class.
		 */
		return MemoryBudget.IN_FIXED_OVERHEAD
				+ IN.computeArraysOverhead(configManager);
	}

	private int IN.computeLsnOverhead() {
		if (entryLsnLongArray == null) {
			return MemoryBudget.byteArraySize(entryLsnByteArray.length);
		} else {
			return MemoryBudget.BYTE_ARRAY_OVERHEAD + entryLsnLongArray.length
					* MemoryBudget.LONG_OVERHEAD;
		}
	}

	static long IN.computeArraysOverhead(DbConfigManager configManager)
			throws DatabaseException {

		/* Count three array elements: states, Keys, and Nodes */
		int capacity = configManager.getInt(EnvironmentParams.NODE_MAX);
		return MemoryBudget.byteArraySize(capacity) + // state array
				(capacity * (2 * MemoryBudget.ARRAY_ITEM_OVERHEAD)); // keys
		// +
		// nodes
	}

	/* Overridden by subclasses. */
	long IN.getMemoryOverhead(MemoryBudget mb) {// MB
		return mb.getINOverhead();
	}

	void IN.updateMemorySize(ChildReference oldRef, ChildReference newRef) {// MB
		long delta = 0;
		if (newRef != null) {
			delta = getEntryInMemorySize(newRef.getKey(), newRef.getTarget());
		}

		if (oldRef != null) {
			delta -= getEntryInMemorySize(oldRef.getKey(), oldRef.getTarget());
		}
		changeMemorySize(delta);
	}

	void IN.updateMemorySize(long oldSize, long newSize) {
		long delta = newSize - oldSize;
		changeMemorySize(delta);
	}

	void IN.updateMemorySize(Node oldNode, Node newNode) {
		long delta = 0;
		if (newNode != null) {
			delta = newNode.getMemorySizeIncludedByParent();
		}

		if (oldNode != null) {
			delta -= oldNode.getMemorySizeIncludedByParent();
		}
		changeMemorySize(delta);
	}

	private void IN.changeMemorySize(long delta) {// MB
		inMemorySize += delta;

		/*
		 * Only update the environment cache usage stats if this IN is actually
		 * on the IN list. For example, when we create new INs, they are
		 * manipulated off the IN list before being added; if we updated the
		 * environment wide cache then, we'd end up double counting.
		 */
		if (inListResident) {
			MemoryBudget mb = databaseImpl.getDbEnvironment().getMemoryBudget();

			accumulatedDelta += delta;
			if (accumulatedDelta > ACCUMULATED_LIMIT
					|| accumulatedDelta < -ACCUMULATED_LIMIT) {
				mb.updateTreeMemoryUsage(accumulatedDelta);
				accumulatedDelta = 0;
			}
		}
	}

	public int IN.getAccumulatedDelta() {// MB
		return accumulatedDelta;
	}
}
