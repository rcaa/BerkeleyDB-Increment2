/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002-2006
 *      Sleepycat Software.  All rights reserved.
 *
 * $Id: TreeIterator.java,v 1.1.6.1.2.1 2006/08/25 08:37:17 ckaestne Exp $
 */

package com.sleepycat.je.tree;

import java.util.Iterator;
import java.util.NoSuchElementException;

import com.sleepycat.je.DatabaseException;

/**
 * TreeIterator implements an Iterator over Tree's. Not protected against
 * insertions like cursors.
 */
public final class TreeIterator implements Iterator {
	private Tree tree;

	private BIN nextBin;

	private int index;

	public TreeIterator(Tree tree) throws DatabaseException {

		nextBin = (BIN) tree.getFirstNode();
		if (nextBin != null) {
//			Lck			nextBin.releaseLatch();
		}
		index = -1;
		this.tree = tree;
	}

	public boolean hasNext() {
		boolean ret = false;
		try {
//			Lck//			if (nextBin != null) {
//				nextBin.latch();
//			}
			advance();
			ret = (nextBin != null) && (index < nextBin.getNEntries());
		} catch (DatabaseException e) {
			// eat exception
//			Lck//		} finally {
//			try {
//				if (nextBin != null) {
//					nextBin.releaseLatch();
//				}
//			} catch (LatchNotHeldException e) {
//				/* Klockwork - ok */
//			}
		}
		return ret;
	}

	public Object next() {

		Object ret = null;
//Lck		try {
			if (nextBin == null) {
				throw new NoSuchElementException();
			}
//			Lck			nextBin.latch();
			ret = nextBin.getKey(index);
//			Lck//		} catch (DatabaseException e) {
//			// eat exception, return null;
//		} finally {
//			try {
//				if (nextBin != null) {
//					nextBin.releaseLatch();
//				}
//			} catch (LatchNotHeldException e) {
//				/* Klockwork - ok */
//			}
//		}
		return ret;
	}

	public void remove() {
		throw new UnsupportedOperationException();
	}

	private void advance() throws DatabaseException {

		while (nextBin != null) {
			if (++index < nextBin.getNEntries()) {
				return;
			}
			nextBin = tree
					.getNextBin(nextBin, false /* traverseWithinDupTree */);
			index = -1;
		}
	}
}
