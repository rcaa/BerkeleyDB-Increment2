/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2000-2006
 *      Sleepycat Software.  All rights reserved.
 *
 * $Id: EnvConfigObserver.java,v 1.1.6.1 2006/07/28 09:02:45 ckaestne Exp $
 */

package com.sleepycat.je.dbi;

import com.sleepycat.je.DatabaseException;

/**
 * Implemented by observers of mutable config changes.
 */
public interface EnvConfigObserver {

	/**
	 * Notifies the observer that one or more mutable properties have been
	 * changed.
	 */
	void envConfigUpdate(DbConfigManager configMgr) throws DatabaseException;
}
