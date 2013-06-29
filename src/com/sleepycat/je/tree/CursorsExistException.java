/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002-2006
 *      Sleepycat Software.  All rights reserved.
 *
 * $Id: CursorsExistException.java,v 1.1.6.1 2006/07/28 09:01:56 ckaestne Exp $
 */

package com.sleepycat.je.tree;

/**
 * Error to indicate that a bottom level BIN has cursors on it during a delete
 * subtree operation.
 */
public class CursorsExistException extends Exception {

	/*
	 * Throw this static instance, in order to reduce the cost of fill in the
	 * stack trace.
	 */
	public static final CursorsExistException CURSORS_EXIST = new CursorsExistException();

	private CursorsExistException() {
	}
}
