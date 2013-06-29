package com.deletedb;

//DELETE DATABASE FEATURE

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.SortedLSNTreeWalker;

/*
 * Calls DatabaseImpl.finishedINListHarvest().
 */
public class SetDbStateSortedLSNTreeWalker extends SortedLSNTreeWalker {

	public SetDbStateSortedLSNTreeWalker(DatabaseImpl dbImpl, boolean removeINsFromINList, long rootLsn, TreeNodeProcessor callback) throws DatabaseException {
		super(dbImpl, removeINsFromINList, rootLsn, callback);
	}

	protected void hook_callHarvest() {
		super.hook_callHarvest();
		dbImpl.finishedINListHarvest();
	}
}
