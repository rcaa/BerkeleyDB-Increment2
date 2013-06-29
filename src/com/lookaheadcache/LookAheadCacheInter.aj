package com.lookaheadcache;

import com.sleepycat.je.cleaner.Cleaner;

public aspect LookAheadCacheInter {

	int Cleaner.lookAheadCacheSize;
}
