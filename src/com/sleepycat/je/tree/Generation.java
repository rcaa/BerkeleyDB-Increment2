/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002-2006
 *      Sleepycat Software.  All rights reserved.
 *
 * $Id: Generation.java,v 1.1.6.1 2006/07/28 09:02:04 ckaestne Exp $
 */

package com.sleepycat.je.tree;

public final class Generation {
	static private long nextGeneration = 0;

	static long getNextGeneration() {
		return nextGeneration++;
	}
}
