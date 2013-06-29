/**
 * 
 */
package com.lookaheadcache;

import java.util.SortedMap;
import java.util.TreeMap;

import com.sleepycat.je.cleaner.LNInfo;

public class LookAheadCache_Count implements LookAheadCache {

	private SortedMap map;

	private int maxMem;

	private int usedMem;

	// size in #items not byte
	LookAheadCache_Count(int lookAheadCacheSize) {
		map = new TreeMap();
		maxMem = lookAheadCacheSize;
		usedMem = 0;
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
		usedMem++;
	}

	public LNInfo remove(Long offset) {
		LNInfo info = (LNInfo) map.remove(offset);
		if (info != null) {
			usedMem--;
		}
		return info;
	}
}