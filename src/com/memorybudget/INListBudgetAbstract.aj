package com.memorybudget;

import com.sleepycat.je.dbi.INList;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;

public privileged aspect INListBudgetAbstract {

	pointcut iNListConstructor(INList inlist) : execution(INList.new(EnvironmentImpl)) && this(inlist);

	pointcut iNListConstructor2(INList inlist) : execution(INList.new(INList, EnvironmentImpl)) && this(inlist);

	pointcut addINList(IN in, INList inlist) : execution(void INList.add(IN)) && args(in) && this(inlist);

	pointcut removeINList(IN in, INList inlist) : execution(void INList.remove(IN)) && args(in) && this(inlist);

	pointcut clearINList(INList inlist) : execution(void INList.clear()) && this(inlist);

	pointcut initIN(IN in) : execution(void IN.init(DatabaseImpl, byte[], int, int)) && this(in);
	
	after(INList inlist) : iNListConstructor(inlist) {
		inlist.updateMemoryUsage = true;
	}

	after(INList inlist) : iNListConstructor2(inlist) {
		inlist.updateMemoryUsage = false;
	}

	after(IN in, INList inlist) : addINList(in, inlist) {
		if (inlist.updateMemoryUsage) {
			MemoryBudget mb = inlist.envImpl.getMemoryBudget();
			mb.updateTreeMemoryUsage(in.getInMemorySize());
			in.setInListResident(true);
		}
	}

	after(IN in, INList inlist):
		removeINList(in, inlist) {
		if (inlist.updateMemoryUsage) {
			MemoryBudget mb = inlist.envImpl.getMemoryBudget();
			mb.updateTreeMemoryUsage(in.getAccumulatedDelta()
					- in.getInMemorySize());
			in.setInListResident(false);
		}

	}

	after(INList inlist):
		clearINList(inlist) {

		if (inlist.updateMemoryUsage) {
			inlist.envImpl.getMemoryBudget().refreshTreeMemoryUsage(0);
		}

	}

	after(IN in) : initIN(in) {
		in.inListResident = false;
	}
	
	private boolean INList.updateMemoryUsage;
	
	private boolean IN.inListResident; // true if this IN is on the IN list

	public void IN.setInListResident(boolean resident) {
		inListResident = resident;
	}
}
