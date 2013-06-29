package com.memorybudget;

import java.util.SortedMap;
import com.sleepycat.je.cleaner.FileProcessor;
import com.sleepycat.je.cleaner.LNInfo;
import com.sleepycat.je.cleaner.PackedOffsets;
import com.sleepycat.je.cleaner.TrackedFileSummary;
import com.sleepycat.je.cleaner.UtilizationProfile;
import com.sleepycat.je.cleaner.UtilizationTracker;
import com.lookaheadcache.LookAheadCache;
import com.sleepycat.je.DatabaseException;
import java.util.TreeMap;
import com.sleepycat.je.cleaner.OffsetList;
import com.lookaheadcache.LookAheadCache_Count;
import java.util.Map;

public privileged aspect CleanerBudgetAbstract {

	pointcut hookr_processFileInternal(FileProcessor cp,
			PackedOffsets obsoleteOffsets, long readBufferSize,
			int lookAheadCacheSize) 
	: execution(boolean FileProcessor.hookr_processFileInternal(Long, PackedOffsets, TrackedFileSummary, PackedOffsets.Iterator, long, int)) &&
	this(cp) && args(Long,obsoleteOffsets,TrackedFileSummary, PackedOffsets.Iterator,readBufferSize, lookAheadCacheSize);

	pointcut lookAheadCacheConstructor(int lookAheadCacheSize) : call(LookAheadCache_Count.new(*)) && args(lookAheadCacheSize);

	pointcut reset(TrackedFileSummary tfs) : execution(void TrackedFileSummary.reset()) && this(tfs);

	pointcut add(TrackedFileSummary tfs) : call(boolean OffsetList.add(long, boolean)) && this(tfs);

	pointcut offsetListConstructor(TrackedFileSummary tfs) : call(OffsetList.new()) && this(tfs);

	pointcut merge(TrackedFileSummary tfs) : call(boolean OffsetList.merge(OffsetList)) && this(tfs);

	pointcut clearCache(UtilizationProfile up) : execution(void UtilizationProfile.clearCache()) && this(up);

	pointcut put(UtilizationProfile lm) : call(Object Map.put(Object, Object)) && target(UtilizationProfile.FileSummaryMap) && this(lm) && 
	withincode(PackedOffsets UtilizationProfile.putFileSummary(TrackedFileSummary));

	pointcut remove(UtilizationProfile lm) : call(Object Map.remove(Object)) && target(UtilizationProfile.FileSummaryMap) && this(lm) && 
	!withincode(boolean UtilizationProfile.populateCache());

	pointcut populateCache(UtilizationProfile up) : execution(boolean UtilizationProfile.populateCache()) && this(up);

	pointcut evictMemory(UtilizationTracker ut) : execution(long UtilizationTracker.evictMemory()) && this(ut);
	
	boolean around(FileProcessor cp, PackedOffsets obsoleteOffsets,
			long readBufferSize, int lookAheadCacheSize):
				hookr_processFileInternal(cp, obsoleteOffsets, readBufferSize, lookAheadCacheSize) {
		long adjustMem = (2 * readBufferSize) + obsoleteOffsets.getLogSize()// MB
				+ lookAheadCacheSize;
		MemoryBudget budget = cp.env.getMemoryBudget();// MB
		budget.updateMiscMemoryUsage(adjustMem);

		boolean result = false;
		try {
			result = proceed(cp, obsoleteOffsets, readBufferSize,
					lookAheadCacheSize);
		} finally {
			/* Subtract the overhead of this method from the budget. */
			budget.updateMiscMemoryUsage(0 - adjustMem);// MB
		}
		return result;
	}

	Object around(int lookAheadCacheSize):
		lookAheadCacheConstructor(lookAheadCacheSize) {
		return new LookAheadCache_MemoryBudget(lookAheadCacheSize);
	}

	after(TrackedFileSummary tfs) : reset(tfs){
		if (tfs.memSize > 0) {
			tfs.updateMemoryBudget(0 - tfs.memSize);
		}
	}

	after(TrackedFileSummary tfs) : add(tfs){
		tfs.updateMemoryBudget(MemoryBudget.TFS_LIST_SEGMENT_OVERHEAD);
	}

	after(TrackedFileSummary tfs) : offsetListConstructor(tfs) {
		tfs.updateMemoryBudget(MemoryBudget.TFS_LIST_INITIAL_OVERHEAD);
	}

	after(TrackedFileSummary tfs) returning (boolean result):
		merge(tfs) {
		if (result)
			tfs.updateMemoryBudget(-MemoryBudget.TFS_LIST_SEGMENT_OVERHEAD);
	}

	before(UtilizationProfile up):
		clearCache(up) {
		int memorySize = up.fileSummaryMap.size()
				* MemoryBudget.UTILIZATION_PROFILE_ENTRY;
		MemoryBudget mb = up.env.getMemoryBudget();
		mb.updateMiscMemoryUsage(0 - memorySize);
	}

	after(UtilizationProfile lm) returning (Object r):
		put(lm) {
		if (r == null) {
			MemoryBudget mb = lm.env.getMemoryBudget();// MB
			mb.updateMiscMemoryUsage(MemoryBudget.UTILIZATION_PROFILE_ENTRY);
		}
	}

	after(UtilizationProfile lm) returning (Object r):
		remove(lm) {
		if (r != null) {
			MemoryBudget mb = lm.env.getMemoryBudget();
			mb.updateMiscMemoryUsage(0 - MemoryBudget.UTILIZATION_PROFILE_ENTRY);
		}
	}

	boolean around(UtilizationProfile up):
		populateCache(up) {
		int oldMemorySize = up.fileSummaryMap.size()
				* MemoryBudget.UTILIZATION_PROFILE_ENTRY;

		boolean r = true;
		try {
			r = proceed(up);
		} finally {
			int newMemorySize = up.fileSummaryMap.size()// MB
					* MemoryBudget.UTILIZATION_PROFILE_ENTRY;
			MemoryBudget mb = up.env.getMemoryBudget();
			mb.updateMiscMemoryUsage(newMemorySize - oldMemorySize);

		}
		return r;
	}

	long around(UtilizationTracker ut) throws DatabaseException:
		evictMemory(ut) {
		/* If not tracking detail, there is nothing to evict. */
		if (!ut.cleaner.trackDetail) {
			return 0;
		}

		/*
		 * Do not start eviction until after recovery, since the
		 * UtilizationProfile will not be initialized properly. UP
		 * initialization requires that all LNs have been replayed.
		 */
		if (!ut.env.isOpen()) {
			return 0;
		}

		MemoryBudget mb = ut.env.getMemoryBudget();
		long totalEvicted = proceed(ut);
		long totalBytes = 0;
		int largestBytes = 0;
		TrackedFileSummary bestFile = null;

		/*
		 * Use a local variable to access the array since the snapshot field can
		 * be changed by other threads.
		 */
		TrackedFileSummary[] a = ut.snapshot;
		for (int i = 0; i < a.length; i += 1) {

			TrackedFileSummary tfs = a[i];
			int mem = tfs.getMemorySize();
			totalBytes += mem;

			if (mem > largestBytes && tfs.getAllowFlush()) {
				largestBytes = mem;
				bestFile = tfs;
			}
		}

		if (bestFile != null && totalBytes > mb.getTrackerBudget()) {
			ut.env.getUtilizationProfile().flushFileSummary(bestFile);
			totalEvicted += largestBytes;
		}
		return totalEvicted;// MB
	}
	
	private static class LookAheadCache_MemoryBudget implements LookAheadCache {

		private SortedMap map;

		private int maxMem;

		private int usedMem;

		LookAheadCache_MemoryBudget(int lookAheadCacheSize) {
			map = new TreeMap();
			maxMem = lookAheadCacheSize;
			usedMem = MemoryBudget.TREEMAP_OVERHEAD;
		}

		public boolean isEmpty() {
			return map.isEmpty();
		}

		public boolean isFull() {
			return usedMem >= maxMem;
		}

		public Long nextOffset() {
			return (Long) map.firstKey();
		}

		public void add(Long lsnOffset, LNInfo info) {
			map.put(lsnOffset, info);
			usedMem += info.getMemorySize();
			usedMem += MemoryBudget.TREEMAP_ENTRY_OVERHEAD;
		}

		public LNInfo remove(Long offset) {
			LNInfo info = (LNInfo) map.remove(offset);
			if (info != null) {
				usedMem -= info.getMemorySize();
				usedMem -= MemoryBudget.TREEMAP_ENTRY_OVERHEAD;
			}
			return info;
		}
	}
	
	int LNInfo.getMemorySize() {
		int size = MemoryBudget.LN_INFO_OVERHEAD;
		if (ln != null) {
			size += ln.getMemorySizeIncludedByParent();
		}
		if (key != null) {
			size += MemoryBudget.byteArraySize(key.length);
		}
		if (dupKey != null) {
			size += MemoryBudget.byteArraySize(dupKey.length);
		}
		return size;
	}

	private int TrackedFileSummary.memSize;// MB

	/**
	 * Return the total memory size for this object. We only bother to budget
	 * obsolete detail, not the overhead for this object, for two reasons: 1)
	 * The number of these objects is very small, and 2) unit tests disable
	 * detail tracking as a way to prevent budget adjustments here.
	 */
	int TrackedFileSummary.getMemorySize() {// MB
		return memSize;
	}

	private void TrackedFileSummary.updateMemoryBudget(int delta) {// MB
		memSize += delta;
		tracker.getEnvironment().getMemoryBudget().updateMiscMemoryUsage(delta);
	}
}
