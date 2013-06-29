package com.memorybudget;

import com.sleepycat.je.dbi.SortedLSNTreeWalker;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.dbi.INList;
import java.util.Iterator;
import java.util.Set;

public privileged aspect TreeWalkerBudgetAbstract {

	pointcut hook_findInternal(SortedLSNTreeWalker tw) : call(void Iterator.remove()) && 
	 withincode(void SortedLSNTreeWalker.hook_findInternal(INList,Set)) && this(tw);
	
	after(SortedLSNTreeWalker tw) returning(Object r) : hook_findInternal(tw) {
		IN thisIN = (IN) r;
		long memoryChange = (thisIN.getAccumulatedDelta() - thisIN
				.getInMemorySize());
		thisIN.setInListResident(false);
		updateTreeMemoryUsage(tw, memoryChange);
	}
	
	private void updateTreeMemoryUsage(SortedLSNTreeWalker tw, long memoryChange) {
		MemoryBudget mb=tw.envImpl.getMemoryBudget();
		mb.updateTreeMemoryUsage(memoryChange);
	}
}