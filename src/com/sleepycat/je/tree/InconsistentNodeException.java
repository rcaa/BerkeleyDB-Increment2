/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002-2006
 *      Sleepycat Software.  All rights reserved.
 *
 * $Id: InconsistentNodeException.java,v 1.1.6.1 2006/07/28 09:01:58 ckaestne Exp $
 */

package com.sleepycat.je.tree;

import com.sleepycat.je.DatabaseException;

/**
 * Error to indicate that something is out of wack in the tree.
 */
public class InconsistentNodeException extends DatabaseException {
	public InconsistentNodeException() {
		super();
	}

	public InconsistentNodeException(String message) {
		super(message);
	}
}
