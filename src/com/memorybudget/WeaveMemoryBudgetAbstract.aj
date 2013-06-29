package com.memorybudget;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentMutableConfig;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.config.IntConfigParam;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.LogBufferBudget;
import com.sleepycat.je.EnvironmentConfig;
import java.io.File;

public privileged aspect WeaveMemoryBudgetAbstract {

	pointcut logBufferBudgetConstructor(EnvironmentImpl env,
			DbConfigManager configManager) 
	 : call(LogBufferBudget.new(EnvironmentImpl,DbConfigManager)) && args(env, configManager);

	pointcut openEnvironmentImpl(EnvironmentImpl env) : call(void EnvironmentImpl.open()) && 
	withincode(EnvironmentImpl.new(File, EnvironmentConfig)) && this(env);

	pointcut fillInEnvironmentGeneratedProps(EnvironmentImpl envImpl,
			EnvironmentMutableConfig p) 
	 : execution(void EnvironmentMutableConfig.fillInEnvironmentGeneratedProps(EnvironmentImpl)) 
	 && args(envImpl) && this(p);
	
	LogBufferBudget around(EnvironmentImpl env, DbConfigManager configManager)
			throws DatabaseException : logBufferBudgetConstructor(env, configManager) {
		return new MemoryBudget(env, configManager);
	}

	before(EnvironmentImpl env) throws DatabaseException : openEnvironmentImpl(env) {
		/* Initialize the environment memory usage number. */
		env.getMemoryBudget().initCacheMemoryUsage();
	}

	before(EnvironmentImpl envImpl, EnvironmentMutableConfig p) : 
		fillInEnvironmentGeneratedProps(envImpl, p) {
		p.cacheSize = envImpl.getMemoryBudget().getMaxMemory();
	}
	
	/* @deprecated As of 2.0, eviction is performed in-line. */
	public static final IntConfigParam EnvironmentParams.EVICTOR_CRITICAL_PERCENTAGE = new IntConfigParam(
			"je.evictor.criticalPercentage", new Integer(0), // min
			new Integer(1000), // max
			new Integer(0), // default
			false, // mutable
			"# At this percentage over the allotted cache, critical eviction\n"
					+ "# will start."
					+ "# (deprecated, eviction is performed in-line");

	public static final IntConfigParam EnvironmentParams.CLEANER_DETAIL_MAX_MEMORY_PERCENTAGE = new IntConfigParam(
			"je.cleaner.detailMaxMemoryPercentage",
			new Integer(1), // min
			new Integer(90), // max
			new Integer(2), // default
			true, // mutable
			"# Tracking of detailed cleaning information will use no more than\n"
					+ "# this percentage of the cache.  The default value is two percent.\n"
					+ "# This setting is only used if je.cleaner.trackDetail=true.");
	
	public MemoryBudget EnvironmentImpl.getMemoryBudget() {
		return (MemoryBudget) memoryBudget;
	}

	/**
	 * Returns the current memory usage in bytes for all btrees in the
	 * environmentImpl.
	 */
	long Environment.getMemoryUsage() throws DatabaseException {
		checkHandleIsValid();
		checkEnv();

		return environmentImpl.getMemoryBudget().getCacheMemoryUsage();
	}
	
	/**
	 * Javadoc for this public method is generated via the doc templates in the
	 * doc_src directory.
	 */
	public long EnvironmentMutableConfig.getCacheSize() {

		/*
		 * CacheSize is filled in from the EnvironmentImpl by way of
		 * copyHandleProps.
		 */
		return cacheSize;
	}

	/*
	 * Cache size is a category of property that is calculated within the
	 * environment. It will be mutable in the future. It is only supplied when
	 * returning the cache size to the application and never used internally;
	 * internal code directly checks with the MemoryBudget class;
	 */
	long EnvironmentMutableConfig.cacheSize;
}
