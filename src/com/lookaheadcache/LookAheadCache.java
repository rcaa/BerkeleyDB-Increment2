package com.lookaheadcache;

import com.sleepycat.je.cleaner.LNInfo;

/**
 * A cache of LNInfo by LSN offset. Used to hold a set of LNs that are to be
 * processed. Keeps track of memory used, and when full (over budget) the next
 * offset should be queried and removed.
 */

public interface LookAheadCache {
	boolean isEmpty();

	boolean isFull();

	Long nextOffset();

	void add(Long lsnOffset, LNInfo info);

	LNInfo remove(Long offset);
}