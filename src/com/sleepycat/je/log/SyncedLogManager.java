/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002-2006
 *      Sleepycat Software.  All rights reserved.
 *
 * $Id: SyncedLogManager.java,v 1.1.6.1.2.1 2006/08/25 08:37:43 ckaestne Exp $
 */

package com.sleepycat.je.log;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.cleaner.TrackedFileSummary;
import com.sleepycat.je.cleaner.UtilizationTracker;
import com.sleepycat.je.dbi.EnvironmentImpl;

/**
 * The SyncedLogManager uses the synchronized keyword to implement protected
 * regions.
 */
public class SyncedLogManager extends LogManager {

	/**
	 * There is a single log manager per database environment.
	 */
	public SyncedLogManager(EnvironmentImpl envImpl, boolean readOnly)
			throws DatabaseException {

		super(envImpl, readOnly);
	}
	
//	Lck replaced sync to logWriteLatch with sync to this

	protected LogResult logItem(LoggableObject item, boolean isProvisional,
			boolean flushRequired, boolean forceNewLogFile, long oldNodeLsn,
			boolean marshallOutsideLatch, ByteBuffer marshalledBuffer,
			UtilizationTracker tracker) throws IOException, DatabaseException {

		synchronized (this) {
			return logInternal(item, isProvisional, flushRequired,
					forceNewLogFile, oldNodeLsn, marshallOutsideLatch,
					marshalledBuffer, tracker);
		}
	}

	protected void flushInternal() throws LogException, DatabaseException {

		try {
			synchronized (this) {
				logBufferPool.writeBufferToFile(0);
			}
		} catch (IOException e) {
			throw new LogException(e.getMessage());
		}
	}

	/**
	 * @see LogManager#getUnflushableTrackedSummary
	 */
	public TrackedFileSummary getUnflushableTrackedSummary(long file)
			throws DatabaseException {

		synchronized (this) {
			return getUnflushableTrackedSummaryInternal(file);
		}
	}

	/**
	 * @see LogManager#countObsoleteLNs
	 */
	public void countObsoleteNode(long lsn, LogEntryType type)
			throws DatabaseException {

		UtilizationTracker tracker = envImpl.getUtilizationTracker();
		synchronized (this) {
			countObsoleteNodeInternal(tracker, lsn, type);
		}
	}

	/**
	 * @see LogManager#countObsoleteNodes
	 */
	public void countObsoleteNodes(TrackedFileSummary[] summaries)
			throws DatabaseException {

		UtilizationTracker tracker = envImpl.getUtilizationTracker();
		synchronized (this) {
			countObsoleteNodesInternal(tracker, summaries);
		}
	}

	/**
	 * @see LogManager#countObsoleteINs
	 */
	public void countObsoleteINs(List lsnList) throws DatabaseException {

		synchronized (this) {
			countObsoleteINsInternal(lsnList);
		}
	}
}
