/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002-2006
 *      Sleepycat Software.  All rights reserved.
 *
 * $Id: INList.java,v 1.1.6.1.2.3 2006/10/25 17:21:26 ckaestne Exp $
 */

package com.sleepycat.je.dbi;

import java.util.Iterator;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.tree.IN;

/**
 * The INList is a list of in-memory INs for a given environment.
 */
//Lck completely restructured
public class INList {
	private static final String DEBUG_NAME = INList.class.getName();

	private SortedSet ins = null;

	// private Set addedINs = null;

	private EnvironmentImpl envImpl;



	INList(EnvironmentImpl envImpl) {
		this.envImpl = envImpl;
		ins = new TreeSet();
	}

	/**
	 * Used only by tree verifier when validating INList. Must be called with
	 * orig.majorLatch acquired.
	 */
	public INList(INList orig, EnvironmentImpl envImpl)
			throws DatabaseException {

		ins = new TreeSet(orig.getINs());
		this.envImpl = envImpl;
	}

	/*
	 * We ignore latching on this method because it's only called from validate
	 * which ignores latching anyway.
	 */
	public SortedSet getINs() {
		return ins;
	}

	/*
	 * Don't require latching, ok to be imprecise.
	 */
	public int getSize() {
		return ins.size();
	}

	/**
	 * An IN has just come into memory, add it to the list.
	 */
	public void add(IN in) throws DatabaseException {

		boolean addOk = ins.add(in);

		assert addOk : "failed adding in " + in.getNodeId();
	}


	/**
	 * An IN is getting swept or is displaced by recovery.
	 */
	public void remove(IN in) throws DatabaseException {

		boolean removeDone = hook_doRemove(in);

		assert removeDone;


	}

	private boolean hook_doRemove(IN in) throws DatabaseException {
		return ins.remove(in);
	}

	public SortedSet tailSet(IN in) throws DatabaseException {

		return ins.tailSet(in);
	}

	public IN first() throws DatabaseException {

		return (IN) ins.first();
	}

	/**
	 * Return an iterator over the main 'ins' set. Returned iterator will not
	 * show the elements in addedINs.
	 * 
	 * The major latch should be held before entering. The caller is responsible
	 * for releasing the major latch when they're finished with the iterator.
	 * 
	 * @return an iterator over the main 'ins' set.
	 */
	public Iterator iterator() {
		return ins.iterator();
	}

	/**
	 * Clear the entire list during recovery and at shutdown.
	 */
	public void clear() throws DatabaseException {

		hook_doClear(ins);


	}

	private void hook_doClear(SortedSet ins) throws DatabaseException {
		ins.clear();
	}

	public void dump() {
		System.out.println("size=" + getSize());
		Iterator iter = ins.iterator();
		while (iter.hasNext()) {
			IN theIN = (IN) iter.next();
			System.out.println("db=" + theIN.getDatabase().getId() + " nid=: "
					+ theIN.getNodeId() + "/" + theIN.getLevel());
		}
	}
	

}
