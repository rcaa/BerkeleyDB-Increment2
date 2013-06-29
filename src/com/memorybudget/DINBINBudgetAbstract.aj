package com.memorybudget;

import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.ChildReference;
import com.sleepycat.je.tree.DIN;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.tree.Node;
import com.sleepycat.je.dbi.DatabaseImpl;

public privileged aspect DINBINBudgetAbstract {

	pointcut dINConstructor(DIN din) : execution(DIN.new(DatabaseImpl, byte[], int, byte[], ChildReference, int)) 
	&& this(din);

	pointcut setDupCountLN(DIN din, ChildReference dupCountLNRef) : execution(void DIN.setDupCountLN(ChildReference)) && args(dupCountLNRef) && this(din);

	pointcut updateDupCountLNOrupdateDupCountLNRefAndNullTarget(DIN din) : (execution(void DIN.updateDupCountLN(Node)) ||
	execution(void DIN.updateDupCountLNRefAndNullTarget(long))) && this(din);

	pointcut setKnownDeleted(int index, BIN bin) : execution(void BIN.setKnownDeleted(int)) && args(index) && this(bin);

	pointcut evictLNs(BIN bin) : execution(long BIN.evictLNs()) && this(bin);

	pointcut evictInternal(BIN bin) : call(long BIN.evictInternal(..)) && withincode(* BIN.evictLN(..)) && this(bin);

	pointcut fetchTarget(IN in, ChildReference cr) : execution(Node ChildReference.fetchTarget(DatabaseImpl, IN)) &&
	args(DatabaseImpl,in) && this(cr) && within(ChildReference);

	after(DIN din):
		dINConstructor(din) {
		din.initMemorySize(); // MB// init after adding Dup Count LN. */
	}

	before(DIN din, ChildReference dupCountLNRef):
		setDupCountLN(din, dupCountLNRef) {
		din.updateMemorySize(din.dupCountLNRef, dupCountLNRef);
	}

	void around(DIN din):
		updateDupCountLNOrupdateDupCountLNRefAndNullTarget(din){
		long oldSize = din.getEntryInMemorySize(din.dupCountLNRef.getKey(),
				din.dupCountLNRef.getTarget());
		proceed(din);
		long newSize = din.getEntryInMemorySize(din.dupCountLNRef.getKey(),
				din.dupCountLNRef.getTarget());
		din.updateMemorySize(oldSize, newSize);// MB

	}

	before(int index, BIN bin) :
		setKnownDeleted(index, bin) {
		bin.updateMemorySize(bin.getTarget(index), null);// MB
	}

	after(BIN bin) returning(long removed) :
		evictLNs(bin) {

		bin.updateMemorySize(removed, 0);// MB
	}

	after(BIN bin) returning(long removed) :
		evictInternal(bin) {
		bin.updateMemorySize(removed, 0);// MB
	}

	Node around(IN in, ChildReference cr):
		fetchTarget(in, cr)	{
		boolean wasNull = cr.target != null;
		Node result = proceed(in, cr);
		if (wasNull) {
			if (in != null) {
				in.updateMemorySize(null, cr.target);// MB
			}
		}
		return result;
	}
}
