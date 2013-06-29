package com.memorybudget;

import com.evictor.Evictor;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.tree.IN;

public privileged aspect EvictorBudgetAbstract {

	pointcut isRunnable(Evictor evictor, String source) : execution(boolean Evictor.isRunnable(String)) && this(evictor) && args(source);

	pointcut hook_getSize(IN renewedChild) : execution(long Evictor.hook_getSize(IN)) && args(renewedChild);
	
	boolean around(Evictor evictor, String source) throws DatabaseException :
		isRunnable(evictor, source) {
		return proceed(evictor, source) || evictor.isRunnableMB(source);
	}

	long around(IN renewedChild): 
		hook_getSize(renewedChild) {
		return renewedChild.getInMemorySize();
	}
	
	// introduces for easier writing (not necessary to programm in third person
	// mode)
	boolean Evictor.isRunnableMB(String source) throws DatabaseException {
		MemoryBudget mb = envImpl.getMemoryBudget();// MB
		long currentUsage = mb.getCacheMemoryUsage();// MB
		long maxMem = mb.getCacheBudget();
		boolean doRun = ((currentUsage - maxMem) > 0);

		/* If running, figure out how much to evict. */
		if (doRun) {
			currentRequiredEvictBytes = (currentUsage - maxMem)
					+ evictBytesSetting;
			if (DEBUG) {
				if (source == SOURCE_CRITICAL) {
					System.out.println("executed: critical runnable");
				}
			}
		}

		/* unit testing, force eviction */
		if (runnableHook != null) {
			doRun = ((Boolean) runnableHook.getHookValue()).booleanValue();
			currentRequiredEvictBytes = maxMem;
		}

		/*
		 * This trace message is expensive, only generate if tracing at this
		 * level is enabled.
		 */
		// Logger logger = envImpl.getLogger();
		// if (logger.isLoggable(detailedTraceLevel)) {
		/*
		 * Generate debugging output. Note that Runtime.freeMemory fluctuates
		 * over time as the JVM grabs more memory, so you really have to do
		 * totalMemory - freeMemory to get stack usage. (You can't get the
		 * concept of memory available from free memory.)
		 */
		/*
		 * Runtime r = Runtime.getRuntime(); long totalBytes = r.totalMemory();
		 * long freeBytes= r.freeMemory(); long usedBytes = r.totalMemory() -
		 * r.freeMemory(); StringBuffer sb = new StringBuffer(); sb.append("
		 * source=").append(source); sb.append(" doRun=").append(doRun);
		 * sb.append(" JEusedBytes=").append(formatter.format(currentUsage));
		 * sb.append(" requiredEvict=").
		 * append(formatter.format(currentRequiredEvictBytes)); sb.append("
		 * JVMtotalBytes= ").append(formatter.format(totalBytes)); sb.append("
		 * JVMfreeBytes= ").append(formatter.format(freeBytes)); sb.append("
		 * JVMusedBytes= ").append(formatter.format(usedBytes));
		 * logger.log(detailedTraceLevel, sb.toString()); }
		 */

		return doRun;
	}
}
