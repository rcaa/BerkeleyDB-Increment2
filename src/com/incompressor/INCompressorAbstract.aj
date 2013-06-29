package com.incompressor;

import java.util.Collection;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.config.IntConfigParam;
import com.sleepycat.je.config.LongConfigParam;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.BINReference;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.tree.Key;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.Environment;
import com.sleepycat.je.utilint.PropUtil;

public privileged aspect INCompressorAbstract {

	pointcut createDaemons(EnvironmentImpl env) : execution(void EnvironmentImpl.createDaemons()) && this(env);

	pointcut runOrPauseDaemons(EnvironmentImpl env, DbConfigManager mgr) : execution(void EnvironmentImpl.runOrPauseDaemons(DbConfigManager)) && args(mgr) && this(env);

	pointcut addToCompressorQueue1(BIN bin, Key deletedKey, boolean doWakeup,
			EnvironmentImpl env) : execution(void EnvironmentImpl.addToCompressorQueue(BIN, Key, boolean )) && 
		args(bin,deletedKey,doWakeup) && this(env);

	pointcut addToCompressorQueue3(BINReference binRef, boolean doWakeup,
			EnvironmentImpl env) : execution(void EnvironmentImpl.addToCompressorQueue(BINReference, boolean)) && args(binRef,doWakeup) && this(env);

	pointcut addToCompressorQueue2(Collection binRefs, boolean doWakeup,
			EnvironmentImpl env) : execution(void EnvironmentImpl.addToCompressorQueue(Collection, boolean)) && 
		args(binRefs,doWakeup) && this(env);

	pointcut lazyCompress(IN in, EnvironmentImpl env) : execution(void EnvironmentImpl.lazyCompress(IN)) && args(in) && this(env);

	pointcut requestShutdownDaemons(EnvironmentImpl env) : set(boolean EnvironmentImpl.closing) &&
		withincode(void EnvironmentImpl.requestShutdownDaemons()) && this(env);

	pointcut shutdownDaemons(EnvironmentImpl env) : execution(void EnvironmentImpl.shutdownDaemons()) && this(env);

	after(EnvironmentImpl env) throws DatabaseException :
		createDaemons(env){
		/* INCompressor */
		long compressorWakeupInterval = PropUtil
				.microsToMillis(env.configManager
						.getLong(EnvironmentParams.COMPRESSOR_WAKEUP_INTERVAL));
		env.inCompressor = new INCompressor(env, compressorWakeupInterval,
				"INCompressor");
	}

	before(EnvironmentImpl env, DbConfigManager mgr) throws DatabaseException:
		runOrPauseDaemons(env, mgr) {
		if (!env.isReadOnly) {
			/* INCompressor */
			env.inCompressor.runOrPause(mgr
					.getBoolean(EnvironmentParams.ENV_RUN_INCOMPRESSOR));
		}
	}

	/**
	 * Tells the asynchronous IN compressor thread about a BIN with a deleted
	 * entry.
	 */
	after(BIN bin, Key deletedKey, boolean doWakeup, EnvironmentImpl env)
			throws DatabaseException :
				addToCompressorQueue1(bin, deletedKey, doWakeup, env) {
		/*
		 * May be called by the cleaner on its last cycle, after the compressor
		 * is shut down.
		 */
		if (env.inCompressor != null) {
			env.inCompressor.addBinKeyToQueue(bin, deletedKey, doWakeup);
		}
	}

	/**
	 * Tells the asynchronous IN compressor thread about a BINReference with a
	 * deleted entry.
	 */
	after(BINReference binRef, boolean doWakeup, EnvironmentImpl env)
			throws DatabaseException :
				addToCompressorQueue3(binRef, doWakeup, env){
		/*
		 * May be called by the cleaner on its last cycle, after the compressor
		 * is shut down.
		 */
		if (env.inCompressor != null) {
			env.inCompressor.addBinRefToQueue(binRef, doWakeup);
		}
	}

	/**
	 * Tells the asynchronous IN compressor thread about a collections of
	 * BINReferences with deleted entries.
	 */
	after(Collection binRefs, boolean doWakeup, EnvironmentImpl env)
			throws DatabaseException :
				addToCompressorQueue2(binRefs, doWakeup, env){
		/*
		 * May be called by the cleaner on its last cycle, after the compressor
		 * is shut down.
		 */
		if (env.inCompressor != null) {
			env.inCompressor.addMultipleBinRefsToQueue(binRefs, doWakeup);
		}
	}

	/**
	 * Do lazy compression at opportune moments.
	 */
	after(IN in, EnvironmentImpl env) throws DatabaseException :
		lazyCompress(in, env){
		/*
		 * May be called by the cleaner on its last cycle, after the compressor
		 * is shut down.
		 */
		if (env.inCompressor != null) {
			env.inCompressor.lazyCompress(in);
		}
	}

	after(EnvironmentImpl env):
		requestShutdownDaemons(env){
		if (env.inCompressor != null) {
			env.inCompressor.requestShutdown();
		}
	}

	before(EnvironmentImpl env) throws InterruptedException :
		shutdownDaemons(env) {
		env.shutdownINCompressorDaemon();
	}

	/*
	 * IN Compressor
	 */
	public static final LongConfigParam EnvironmentParams.COMPRESSOR_WAKEUP_INTERVAL = new LongConfigParam(
			"je.compressor.wakeupInterval", new Long(1000000), // min
			new Long(4294967296L), // max
			new Long(5000000), // default
			false, // mutable
			"# The compressor wakeup interval in microseconds.");

	public static final IntConfigParam EnvironmentParams.COMPRESSOR_RETRY = new IntConfigParam(
			"je.compressor.deadlockRetry", new Integer(0), // min
			new Integer(Integer.MAX_VALUE),// max
			new Integer(3), // default
			false, // mutable
			"# Number of times to retry a compression run if a deadlock occurs.");

	public static final LongConfigParam EnvironmentParams.COMPRESSOR_LOCK_TIMEOUT = new LongConfigParam(
			"je.compressor.lockTimeout", new Long(0), // min
			new Long(4294967296L), // max
			new Long(500000L), // default
			false, // mutable
			"# The lock timeout for compressor transactions in microseconds.");

	private INCompressor EnvironmentImpl.inCompressor;

	/**
	 * Return the incompressor. In general, don't use this directly because it's
	 * easy to forget that the incompressor can be null at times (i.e during the
	 * shutdown procedure. Instead, wrap the functionality within this class,
	 * like lazyCompress.
	 */
	public INCompressor EnvironmentImpl.getINCompressor() {
		return inCompressor;
	}

	/**
	 * Invoke a compress programatically. Note that only one compress may run at
	 * a time.
	 */
	public boolean EnvironmentImpl.invokeCompressor() throws DatabaseException {

		if (inCompressor != null) {
			inCompressor.doCompress();
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Available for the unit tests.
	 */
	public void EnvironmentImpl.shutdownINCompressorDaemon()
			throws InterruptedException {

		if (inCompressor != null) {
			inCompressor.shutdown();

			/*
			 * If daemon thread doesn't shutdown for any reason, at least clear
			 * the reference to the environment so it can be GC'd.
			 */
			inCompressor.clearEnv();
			inCompressor = null;
		}
		return;
	}

	public int EnvironmentImpl.getINCompressorQueueSize()
			throws DatabaseException {

		return inCompressor.getBinRefQueueSize();
	}

	/**
	 * Javadoc for this public method is generated via the doc templates in the
	 * doc_src directory.
	 */
	public void Environment.compress() throws DatabaseException {

		checkHandleIsValid();
		checkEnv();
		environmentImpl.invokeCompressor();
	}
}