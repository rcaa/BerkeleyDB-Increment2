package com.memorybudget;

import java.util.Iterator;
import java.util.Set;
import java.util.SortedMap;
import com.sleepycat.je.dbi.INList;
import com.sleepycat.je.recovery.Checkpointer;
import com.sleepycat.je.tree.IN;

public privileged aspect CheckpointerBudgetAbstract {

	pointcut hookr_doCheckpointInternal(Checkpointer cp, SortedMap dirtyMap) 
	: execution(void Checkpointer.hookr_doCheckpointInternal(..)) && this(cp) && args(..,dirtyMap);

	pointcut hook_refreshTreeMemoryUsage(Checkpointer cp, INList inMemINs) 
	: execution(void Checkpointer.hook_refreshTreeMemoryUsage(INList)) && this(cp) && args(inMemINs);
	
	void around(Checkpointer cp, SortedMap dirtyMap):
		hookr_doCheckpointInternal(cp, dirtyMap) {

		int dirtyMapMemSize = 0;
		MemoryBudget mb = cp.envImpl.getMemoryBudget();// MB

		try {
			/* Add each level's references to the budget. */
			int totalSize = 0;// MB
			for (Iterator i = dirtyMap.values().iterator(); i.hasNext();) {
				Set nodeSet = (Set) i.next();
				int size = nodeSet.size()
						* MemoryBudget.CHECKPOINT_REFERENCE_SIZE;
				totalSize += size;
				dirtyMapMemSize += size;
			}
			mb.updateMiscMemoryUsage(totalSize);// MB

			proceed(cp, dirtyMap);
		} finally {
			mb.updateMiscMemoryUsage(0 - dirtyMapMemSize);// MB
		}

	}

	before(Checkpointer cp, INList inMemINs):
		hook_refreshTreeMemoryUsage(cp, inMemINs) {

		/* Set the tree cache size. */
		long totalSize = 0;// MB
		MemoryBudget mb = cp.envImpl.getMemoryBudget();// MB

		// try {
		Iterator iter = inMemINs.iterator();
		while (iter.hasNext()) {
			IN in = (IN) iter.next();
			// TODO latch this! (again performanceloss because need to latch
			// this twice now.
			totalSize = mb.accumulateNewUsage(in, totalSize);// MB
		}
		mb.refreshTreeMemoryUsage(totalSize);// MB
	}
}
