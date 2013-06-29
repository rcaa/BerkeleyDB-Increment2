package com.lookaheadcache;

import java.util.Map;
import java.util.Set;

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
import com.sleepycat.je.cleaner.PackedOffsets;

public privileged aspect LookAheadCacheAbstract {

	pointcut processLN(Long fileNum, TreeLocation location, Long offset, LNInfo info, Map map,
			FileProcessor fp) : call(void FileProcessor.processLN(Long, TreeLocation, Long, LNInfo, Map)) && 
			args(fileNum, location, offset, info, map) && this(fp) && 
			withincode(boolean hookr_processFileInternalLoop(Long, TrackedFileSummary, PackedOffsets.Iterator, long, Set, Map, CleanerFileReader, DbTree, TreeLocation));
	
	pointcut hookr_processFileInternalLoop(FileProcessor fp, Long fileNum, Map dbCache, TreeLocation location, TrackedFileSummary trackedFileSummary, long numb, Set set, CleanerFileReader cleanerFileReader, DbTree dbTree) 
	: execution(boolean FileProcessor.hookr_processFileInternalLoop(Long, TrackedFileSummary, PackedOffsets.Iterator, long, Set, Map, CleanerFileReader, DbTree, TreeLocation)) 
	&& this(fp) && args(fileNum, trackedFileSummary, PackedOffsets.Iterator, numb, set, dbCache, cleanerFileReader, dbTree, location);
	
	pointcut envConfigUpdate(Cleaner c, DbConfigManager cm) : execution(void Cleaner.envConfigUpdate(DbConfigManager)) && args(cm) && this(c);
	
	pointcut processFile(FileProcessor fp):
		execution(boolean FileProcessor.processFile(Long)) && this(fp);

	void around(Long fileNum, TreeLocation location, Long offset, LNInfo info,
			Map map, FileProcessor fp) throws DatabaseException:
				processLN(fileNum, location, offset, info, map, fp) {
		if (lookAheadCache == null) {
			lookAheadCache = new LookAheadCache_Count(
					fp.cleaner.lookAheadCacheSize);
		}

		lookAheadCache.add(offset, info);

		if (lookAheadCache.isFull()) {
			Long poffset = lookAheadCache.nextOffset();
			LNInfo pinfo = lookAheadCache.remove(poffset);
			proceed(fileNum, location, poffset, pinfo, map, fp);

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

	after(FileProcessor fp, Long fileNum, Map dbCache, TreeLocation location,
			TrackedFileSummary trackedFileSummary, long numb, Set set,
			CleanerFileReader cleanerFileReader, DbTree dbTree)
			throws DatabaseException:
				hookr_processFileInternalLoop(fp, fileNum, dbCache, location, trackedFileSummary, numb, set, cleanerFileReader, dbTree){

		/* Process remaining queued LNs. */
		while (!lookAheadCache.isEmpty()) {
			fp.hook_beforeProcess();

			Long poffset = lookAheadCache.nextOffset();
			LNInfo pinfo = lookAheadCache.remove(poffset);
			fp.processLN(fileNum, location, poffset, pinfo, dbCache);
		}
	}

	after(Cleaner c, DbConfigManager cm) throws DatabaseException:
		envConfigUpdate(c, cm){
		c.lookAheadCacheSize = cm
				.getInt(EnvironmentParams.CLEANER_LOOK_AHEAD_CACHE_SIZE);
	}

	LookAheadCache lookAheadCache = null;

	int Cleaner.lookAheadCacheSize;
}
