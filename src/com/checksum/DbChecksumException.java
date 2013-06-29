/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002-2006
 *      Sleepycat Software.  All rights reserved.
 *
 * $Id: DbChecksumException.java,v 1.1.2.1 2006/10/12 16:08:23 ckaestne Exp $
 */

package com.checksum;

import com.sleepycat.je.RunRecoveryException;
import com.sleepycat.je.dbi.EnvironmentImpl;

/**
 * Configuration related exceptions.
 */
public class DbChecksumException extends RunRecoveryException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public DbChecksumException(EnvironmentImpl env, String message) {
		super(env, message);
	}

	public DbChecksumException(EnvironmentImpl env, String message, Throwable t) {
		super(env, message, t);
	}
}
