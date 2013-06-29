/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002-2006
 *      Sleepycat Software.  All rights reserved.
 *
 * $Id: RecoveryException.java,v 1.1.6.1 2006/07/28 09:02:54 ckaestne Exp $
 */

package com.sleepycat.je.recovery;

import com.sleepycat.je.RunRecoveryException;
import com.sleepycat.je.dbi.EnvironmentImpl;

/**
 * Recovery related exceptions
 */
public class RecoveryException extends RunRecoveryException {

	public RecoveryException(EnvironmentImpl env, String message, Throwable t) {
		super(env, message, t);
	}

	public RecoveryException(EnvironmentImpl env, String message) {
		super(env, message);
	}
}
