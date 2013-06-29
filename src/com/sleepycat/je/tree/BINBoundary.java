/*
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002-2006
 *      Sleepycat Software.  All rights reserved.
 *
 * $Id: BINBoundary.java,v 1.1.6.1 2006/07/28 09:02:01 ckaestne Exp $:
 */

package com.sleepycat.je.tree;

/**
 * Contains information about the BIN returned by a search.
 */
public class BINBoundary {
	/** The last BIN was returned. */
	public boolean isLastBin;

	/** The first BIN was returned. */
	public boolean isFirstBin;
}
