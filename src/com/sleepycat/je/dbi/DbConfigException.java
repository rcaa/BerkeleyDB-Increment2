/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002-2006
 *      Sleepycat Software.  All rights reserved.
 *
 * $Id: DbConfigException.java,v 1.1.6.1 2006/07/28 09:02:44 ckaestne Exp $
 */

package com.sleepycat.je.dbi;

import com.sleepycat.je.DatabaseException;

/**
 * Configuration related exceptions.
 */
public class DbConfigException extends DatabaseException {

	public DbConfigException(Throwable t) {
		super(t);
	}

	public DbConfigException(String message) {
		super(message);
	}

	public DbConfigException(String message, Throwable t) {
		super(message, t);
	}
}
