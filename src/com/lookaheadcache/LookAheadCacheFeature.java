package com.lookaheadcache;

import java.util.Map;
import java.util.Set;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.cleaner.Cleaner;
import com.sleepycat.je.cleaner.FileProcessor;
import com.sleepycat.je.cleaner.LNInfo;
import com.sleepycat.je.cleaner.TrackedFileSummary;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.log.CleanerFileReader;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.LN;
import com.sleepycat.je.tree.TreeLocation;
import com.sleepycat.je.utilint.DbLsn;

@Aspect
public class LookAheadCacheFeature {

	LookAheadCache lookAheadCache = null;
	
	@Pointcut("call(void com.sleepycat.je.cleaner.FileProcessor.processLN(Long, com.sleepycat.je.tree.TreeLocation, Long, com.sleepycat.je.cleaner.LNInfo, java.util.Map)) && args(fileNum, location, offset, info, map) && this(fp) && withincode(boolean hookr_processFileInternalLoop(Long, com.sleepycat.je.cleaner.TrackedFileSummary, com.sleepycat.je.cleaner.PackedOffsets.Iterator, long, java.util.Set, java.util.Map, com.sleepycat.je.log.CleanerFileReader, com.sleepycat.je.dbi.DbTree, com.sleepycat.je.tree.TreeLocation))")
	public void processLN(Long fileNum, TreeLocation location, Long offset, LNInfo info, Map map, FileProcessor fp) {}
	
	@Around("processLN(fileNum, location, offset, info, map, fp)")
	public void around(Long fileNum, TreeLocation location, Long offset, LNInfo info, Map map, FileProcessor fp, ProceedingJoinPoint pjp) throws Throwable {
		if (lookAheadCache == null) {
			lookAheadCache = new LookAheadCache_Count(
					fp.cleaner.lookAheadCacheSize);
		}

		lookAheadCache.add(offset, info);

		if (lookAheadCache.isFull()) {
			Long poffset = lookAheadCache.nextOffset();
			LNInfo pinfo = lookAheadCache.remove(poffset);
			Object[] objs = new Object[] {fileNum, location, poffset, pinfo, map, fp};
			pjp.proceed(objs);

			LN ln = info.getLN();
			boolean isDupCountLN = ln.containsDuplicates();
			BIN bin = location.bin;
			int index = location.index;

			/*
			 * For all other non-deleted LNs in this BIN, lookup their LSN in
			 * the LN queue and process any matches.
			 */
			if (!isDupCountLN) {
				for (int i = 0; i < bin.getNEntries(); i += 1) {
					long lsn = bin.getLsn(i);
					if (i != index && !bin.isEntryKnownDeleted(i)
							&& !bin.isEntryPendingDeleted(i)
							&& DbLsn.getFileNumber(lsn) == fileNum.longValue()) {

						Long myOffset = new Long(DbLsn.getFileOffset(lsn));
						LNInfo myInfo = lookAheadCache.remove(myOffset);

						if (myInfo != null) {

							fp.processFoundLN(myInfo, lsn, lsn, bin, i, null);
						}
					}
				}
			}
		}
	}
	
	@Pointcut("execution(boolean com.sleepycat.je.cleaner.FileProcessor.hookr_processFileInternalLoop(Long, com.sleepycat.je.cleaner.TrackedFileSummary, com.sleepycat.je.cleaner.PackedOffsets.Iterator, long, java.util.Set, java.util.Map, com.sleepycat.je.log.CleanerFileReader, com.sleepycat.je.dbi.DbTree, com.sleepycat.je.tree.TreeLocation)) && this(fp) && args(fileNum, trackedFileSummary, com.sleepycat.je.cleaner.PackedOffsets.Iterator, numb, set, dbCache, cleanerFileReader, dbTree, location)")
	public void hookr_processFileInternalLoop(FileProcessor fp, Long fileNum, Map dbCache, TreeLocation location, TrackedFileSummary trackedFileSummary, long numb, Set set, CleanerFileReader cleanerFileReader, DbTree dbTree) {}
	
	@After("hookr_processFileInternalLoop(fp, fileNum, dbCache, location, trackedFileSummary, numb, set, cleanerFileReader, dbTree)")
	public void after1(FileProcessor fp, Long fileNum, Map dbCache, TreeLocation location, TrackedFileSummary trackedFileSummary, long numb, Set set, CleanerFileReader cleanerFileReader, DbTree dbTree) throws DatabaseException {
		/* Process remaining queued LNs. */
		while (!lookAheadCache.isEmpty()) {
			fp.hook_beforeProcess();

			Long poffset = lookAheadCache.nextOffset();
			LNInfo pinfo = lookAheadCache.remove(poffset);
			fp.processLN(fileNum, location, poffset, pinfo, dbCache);
		}
	}
	
	@Pointcut("execution(void com.sleepycat.je.cleaner.Cleaner.envConfigUpdate(com.sleepycat.je.dbi.DbConfigManager)) && args(cm) && this(c)")
	public void envConfigUpdate(Cleaner c, DbConfigManager cm) {}
	
	@After("envConfigUpdate(c, cm)")
	public void after2(Cleaner c, DbConfigManager cm) throws DatabaseException {
		c.lookAheadCacheSize = cm
				.getInt(EnvironmentParams.CLEANER_LOOK_AHEAD_CACHE_SIZE);
	}
}
