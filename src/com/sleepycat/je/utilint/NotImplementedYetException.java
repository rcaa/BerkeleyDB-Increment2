/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002-2006
 *      Sleepycat Software.  All rights reserved.
 *
 * $Id: NotImplementedYetException.java,v 1.1.6.1 2006/07/28 09:02:51 ckaestne Exp $
 */

package com.sleepycat.je.utilint;

/**
 * Something is not yet implemented.
 */
public class NotImplementedYetException extends RuntimeException {

	public NotImplementedYetException() {
		super();
	}

	public NotImplementedYetException(String message) {
		super(message);
	}
}
