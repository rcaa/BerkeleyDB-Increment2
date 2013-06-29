package com.incompressor;

import java.util.Collection;

import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.cleaner.UtilizationTracker;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.BINReference;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.tree.Key;
import com.sleepycat.je.tree.Tree;
import com.sleepycat.je.utilint.PropUtil;

@Aspect
public class INCompressorFeature {

	private boolean purgeRoot;
	
	@Pointcut("execution(void com.sleepycat.je.dbi.EnvironmentImpl.createDaemons()) && this(env)")
	public void createDaemons(EnvironmentImpl env) {}
	
	@After("createDaemons(env)")
	public void after1(EnvironmentImpl env) throws DatabaseException {
		/* INCompressor */
		long compressorWakeupInterval = PropUtil
				.microsToMillis(env.configManager
						.getLong(EnvironmentParams.COMPRESSOR_WAKEUP_INTERVAL));
		env.inCompressor = new INCompressor(env, compressorWakeupInterval,
				"INCompressor");
	}
	
	@Pointcut("execution(void com.sleepycat.je.dbi.EnvironmentImpl.runOrPauseDaemons(com.sleepycat.je.dbi.DbConfigManager)) && args(mgr) && this(env)")
	public void runOrPauseDaemons(EnvironmentImpl env, DbConfigManager mgr) {}
	
	@Before("runOrPauseDaemons(env, mgr)")
	public void before1(EnvironmentImpl env, DbConfigManager mgr) throws DatabaseException {
		if (!env.isReadOnly) {
			/* INCompressor */
			env.inCompressor.runOrPause(mgr
					.getBoolean(EnvironmentParams.ENV_RUN_INCOMPRESSOR));
		}
	}
	
	@Pointcut("execution(void com.sleepycat.je.dbi.EnvironmentImpl.addToCompressorQueue(com.sleepycat.je.tree.BIN, com.sleepycat.je.tree.Key, boolean)) && args(bin,deletedKey,doWakeup) && this(env)")
	public void addToCompressorQueue1(BIN bin, Key deletedKey, boolean doWakeup, EnvironmentImpl env) {}
	
	
	@After("addToCompressorQueue1(bin, deletedKey, doWakeup, env)")
	public void after2(BIN bin, Key deletedKey, boolean doWakeup, EnvironmentImpl env) throws DatabaseException {
		/*
		 * May be called by the cleaner on its last cycle, after the compressor
		 * is shut down.
		 */
		if (env.inCompressor != null) {
			env.inCompressor.addBinKeyToQueue(bin, deletedKey, doWakeup);
		}
	}
	
	@Pointcut("execution(void com.sleepycat.je.dbi.EnvironmentImpl.addToCompressorQueue(com.sleepycat.je.tree.BINReference, boolean)) && args(binRef,doWakeup) && this(env)")
	public void addToCompressorQueue3(BINReference binRef, boolean doWakeup, EnvironmentImpl env) {}
	
	@After("addToCompressorQueue3(binRef, doWakeup, env)")
	public void after3(BINReference binRef, boolean doWakeup, EnvironmentImpl env) throws DatabaseException {
		/*
		 * May be called by the cleaner on its last cycle, after the compressor
		 * is shut down.
		 */
		if (env.inCompressor != null) {
			env.inCompressor.addBinRefToQueue(binRef, doWakeup);
		}
	}
	
	@Pointcut("execution(void com.sleepycat.je.dbi.EnvironmentImpl.addToCompressorQueue(Collection, boolean)) && args(binRefs,doWakeup) && this(env)")
	public void addToCompressorQueue2(Collection binRefs, boolean doWakeup, EnvironmentImpl env) {}
	
	@After("addToCompressorQueue2(binRefs, doWakeup, env)")
	public void after4(Collection binRefs, boolean doWakeup, EnvironmentImpl env) throws DatabaseException {
		/*
		 * May be called by the cleaner on its last cycle, after the compressor
		 * is shut down.
		 */
		if (env.inCompressor != null) {
			env.inCompressor.addMultipleBinRefsToQueue(binRefs, doWakeup);
		}
	}
	
	@Pointcut("execution(void com.sleepycat.je.dbi.EnvironmentImpl.lazyCompress(com.sleepycat.je.tree.IN)) && args(in) && this(env)")
	public void lazyCompress(IN in, EnvironmentImpl env) {}
	
	@After("lazyCompress(in, env)")
	public void after5(IN in, EnvironmentImpl env) throws DatabaseException {
		/*
		 * May be called by the cleaner on its last cycle, after the compressor
		 * is shut down.
		 */
		if (env.inCompressor != null) {
			env.inCompressor.lazyCompress(in);
		}
	}
	
	@Pointcut("set(boolean com.sleepycat.je.dbi.EnvironmentImpl.closing) && withincode(void com.sleepycat.je.dbi.EnvironmentImpl.requestShutdownDaemons()) && this(env)")
	public void requestShutdownDaemons(EnvironmentImpl env) {}

	@After("requestShutdownDaemons(env)")
	public void after6(EnvironmentImpl env) {
		if (env.inCompressor != null) {
			env.inCompressor.requestShutdown();
		}
	}
	
	@Pointcut("execution(void com.sleepycat.je.dbi.EnvironmentImpl.shutdownDaemons()) && this(env)")
	public void shutdownDaemons(EnvironmentImpl env) {}
	
	@Before("shutdownDaemons(env)")
	public void before2(EnvironmentImpl env) throws InterruptedException {
		env.shutdownINCompressorDaemon();
	}
	
	@Pointcut("execution(void com.sleepycat.je.tree.Tree.setDatabase(com.sleepycat.je.dbi.DatabaseImpl)) && args(database)")
	public void setDatabase(DatabaseImpl database) {}
	
	@After("setDatabase(database)")
	public void after7(DatabaseImpl database) throws DatabaseException {
		DbConfigManager configManager = database.getDbEnvironment()
				.getConfigManager();
		purgeRoot = configManager
				.getBoolean(EnvironmentParams.COMPRESSOR_PURGE_ROOT);
	}
	
	@Pointcut("execution(com.sleepycat.je.tree.IN com.sleepycat.je.tree.Tree.hook_updateRoot(com.sleepycat.je.tree.IN, com.sleepycat.je.cleaner.UtilizationTracker)) && args(rootIN, tracker) && this(tree)")
	public void hook_updateRoot(IN rootIN, UtilizationTracker tracker, Tree tree) {}
	
	@Around("hook_updateRoot(rootIN, tracker, tree)")
	public IN around1(IN rootIN, UtilizationTracker tracker, Tree tree) throws DatabaseException {
		IN subtreeRootIN = null;
		if (purgeRoot) {
			subtreeRootIN = tree.logTreeRemoval(rootIN, tracker);
		}
		return subtreeRootIN;
	}
	

	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	


}
