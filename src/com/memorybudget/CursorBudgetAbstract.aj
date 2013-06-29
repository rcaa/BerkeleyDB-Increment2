package com.memorybudget;

import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.LN;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.CursorImpl;

public privileged aspect CursorBudgetAbstract {

	pointcut hook_lnDeleteOrhook_lnModify(BIN targetBin, LN ln) : (execution(* CursorImpl.hook_lnDelete(..)) || execution(* CursorImpl.hook_lnModify(..))) && 
	args(targetBin,ln,..);
	
	//TODO homogenic only because of hook methods 
	Object around(BIN targetBin, LN ln) throws DatabaseException :
		hook_lnDeleteOrhook_lnModify(targetBin, ln) {
		long oldLNSize = ln.getMemorySizeIncludedByParent();
		Object r = proceed(targetBin, ln);
		long newLNSize = ln.getMemorySizeIncludedByParent();
		targetBin.updateMemorySize(oldLNSize, newLNSize);
		return r;
	}
}
