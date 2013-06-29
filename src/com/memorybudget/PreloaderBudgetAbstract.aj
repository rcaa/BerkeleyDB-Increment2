package com.memorybudget;

import com.sleepycat.je.PreloadConfig;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DatabaseImpl.HaltPreloadException;
import com.sleepycat.je.dbi.DatabaseImpl.PreloadLSNTreeWalker;
import com.sleepycat.je.dbi.DatabaseImpl.PreloadProcessor;
import com.sleepycat.je.Database;
import com.sleepycat.je.PreloadResult;
import com.sleepycat.je.PreloadStatus;
import com.sleepycat.je.dbi.SortedLSNTreeWalker.TreeNodeProcessor;
import com.sleepycat.je.log.LogEntryType;

public privileged aspect PreloaderBudgetAbstract {

	pointcut preloadLSNTreeWalkerConstructor(DatabaseImpl db,
			PreloadProcessor pp, PreloadConfig config) 
	: call(PreloadLSNTreeWalker.new(DatabaseImpl,TreeNodeProcessor,PreloadConfig)) && 
	args(DatabaseImpl,pp,config) && this(db) &&
	withincode(PreloadResult DatabaseImpl.preload(PreloadConfig));

	pointcut processLSN(PreloadProcessor pp) 
	: execution(void PreloadProcessor.processLSN(long, LogEntryType)) && this(pp);

	pointcut hook_setConfig(PreloadConfig config, long v) 
	: execution(void Database.hook_setConfig(PreloadConfig, long)) && args(config,v);
	
	PreloadLSNTreeWalker around(DatabaseImpl db, PreloadProcessor pp,
			PreloadConfig config) :
		preloadLSNTreeWalkerConstructor(db, pp, config) {
		long maxBytes = config.getMaxBytes();
		long cacheBudget = db.envImpl.getMemoryBudget().getCacheBudget();
		if (maxBytes == 0) {
			maxBytes = cacheBudget;
		} else if (maxBytes > cacheBudget) {
			throw new IllegalArgumentException(
					"maxBytes parameter to Database.preload() was specified as "
							+ maxBytes + " bytes \nbut the cache is only "
							+ cacheBudget + " bytes.");
		}

		pp.maxBytes = maxBytes;

		return proceed(db, pp, config);
	}

	after(PreloadProcessor pp) : processLSN(pp) {
		if (pp.maxBytes != 0
				&& pp.envImpl.getMemoryBudget().getCacheMemoryUsage() > pp.maxBytes) {
			throw memoryExceededPreloadException;
		}
	}

	before(PreloadConfig config, long v) : hook_setConfig(config, v) {
		config.setMaxBytes(v);
	}

	private long PreloadProcessor.maxBytes = 0;// MB

	private static final HaltPreloadException memoryExceededPreloadException = new HaltPreloadException(
			PreloadStatus.FILLED_CACHE);

	private long PreloadConfig.maxBytes;

	/**
	 * Javadoc for this public method is generated via the doc templates in the
	 * doc_src directory.
	 */
	public long PreloadConfig.getMaxBytes() {
		return maxBytes;
	}

	/**
	 * Javadoc for this public method is generated via the doc templates in the
	 * doc_src directory.
	 */
	public void PreloadConfig.setMaxBytes(long maxBytes) {
		this.maxBytes = maxBytes;
	}
}