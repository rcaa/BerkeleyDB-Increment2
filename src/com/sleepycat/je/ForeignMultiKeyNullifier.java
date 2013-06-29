/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002-2006
 *      Sleepycat Software.  All rights reserved.
 *
 * $Id: ForeignMultiKeyNullifier.java,v 1.1.6.1 2006/07/28 09:02:24 ckaestne Exp $
 */

package com.sleepycat.je;

/**
 * Javadoc for this public method is generated via the doc templates in the
 * doc_src directory.
 */
public interface ForeignMultiKeyNullifier {

	/**
	 * Javadoc for this public method is generated via the doc templates in the
	 * doc_src directory.
	 */
	public boolean nullifyForeignKey(SecondaryDatabase secondary,
			DatabaseEntry key, DatabaseEntry data, DatabaseEntry secKey)
			throws DatabaseException;
}
