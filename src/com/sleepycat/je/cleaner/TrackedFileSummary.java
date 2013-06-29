/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002-2006
 *      Sleepycat Software.  All rights reserved.
 *
 * $Id: TrackedFileSummary.java,v 1.1.6.1.2.4 2006/10/25 17:21:29 ckaestne Exp $
 */

package com.sleepycat.je.cleaner;

import com.sleepycat.je.dbi.LogBufferBudget;

/**
 * Delta file summary info for a tracked file. Tracked files are managed by the
 * UtilizationTracker.
 * 
 * <p>
 * The methods in this class for reading obsolete offsets may be used by
 * multiple threads without synchronization even while another thread is adding
 * offsets. This is possible because elements are never deleted from the lists.
 * The thread adding obsolete offsets does so under the log write latch to
 * prevent multiple threads from adding concurrently.
 * </p>
 */
public class TrackedFileSummary extends FileSummary {

	private UtilizationTracker tracker;

	private long fileNum;

	private OffsetList obsoleteOffsets;


	private boolean trackDetail;

	private boolean allowFlush = true;

	/**
	 * Creates an empty tracked summary.
	 */
	TrackedFileSummary(UtilizationTracker tracker, long fileNum,
			boolean trackDetail) {
		this.tracker = tracker;
		this.fileNum = fileNum;
		this.trackDetail = trackDetail;
	}

	/**
	 * Returns whether this summary is allowed or prohibited from being flushed
	 * or evicted during cleaning. By default, flushing is allowed.
	 */
	public boolean getAllowFlush() {
		return allowFlush;
	}

	/**
	 * Allows or prohibits this summary from being flushed or evicted during
	 * cleaning. By default, flushing is allowed.
	 */
	void setAllowFlush(boolean allowFlush) {
		this.allowFlush = allowFlush;
	}

	/**
	 * Returns the file number being tracked.
	 */
	public long getFileNumber() {
		return fileNum;
	}



	/**
	 * Overrides reset for a tracked file, and is called when a FileSummaryLN is
	 * written to the log.
	 * 
	 * <p>
	 * Must be called under the log write latch.
	 * </p>
	 */
	public void reset() {

		obsoleteOffsets = null;

		tracker.resetFile(this);



		super.reset();
	}

	/**
	 * Tracks the given offset as obsolete or non-obsolete.
	 * 
	 * <p>
	 * Must be called under the log write latch.
	 * </p>
	 */
	void trackObsolete(long offset) {

		if (!trackDetail) {
			return;
		}
		if (obsoleteOffsets == null) {
			obsoleteOffsets = new OffsetList();
		}
		if (obsoleteOffsets.add(offset, tracker.getEnvironment().isOpen())) {
		}
	}

	/**
	 * Adds the obsolete offsets as well as the totals of the given object.
	 */
	void addTrackedSummary(TrackedFileSummary other) {

		/* Add the totals. */
		add(other);

		/*
		 * Add the offsets. The memory budget has already been updated for the
		 * offsets to be added, so we only need to account for a difference when
		 * we merge them.
		 */
		if (other.obsoleteOffsets != null) {
			if (obsoleteOffsets != null) {
				/* Merge the other offsets into our list. */
				obsoleteOffsets.merge(other.obsoleteOffsets);
			} else {
				/* Adopt the other's offsets as our own. */
				obsoleteOffsets = other.obsoleteOffsets;
			}
		}
	}

	/**
	 * Returns obsolete offsets as an array of longs, or null if none.
	 */
	public long[] getObsoleteOffsets() {

		if (obsoleteOffsets != null) {
			return obsoleteOffsets.toArray();
		} else {
			return null;
		}
	}

	/**
	 * Returns whether the given offset is present in the tracked offsets. This
	 * does not indicate whether the offset is obsolete in general, but only if
	 * it is known to be obsolete in this version of the tracked information.
	 */
	boolean containsObsoleteOffset(long offset) {

		if (obsoleteOffsets != null) {
			return obsoleteOffsets.contains(offset);
		} else {
			return false;
		}
	}


}
