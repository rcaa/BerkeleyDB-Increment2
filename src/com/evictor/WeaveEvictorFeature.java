package com.evictor;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.CursorImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.recovery.Checkpointer;
import com.sleepycat.je.recovery.RecoveryManager;
import com.sleepycat.je.recovery.Checkpointer.CheckpointStartResult;

@Aspect
public class WeaveEvictorFeature {

	@Pointcut("execution(void com.sleepycat.je.dbi.EnvironmentImpl.requestShutdownDaemons()) && this(env)")
	public void requestShutdownDaemons(EnvironmentImpl env) {}
	
	@After("requestShutdownDaemons(env)")
	public void after1(EnvironmentImpl env) {
		if (env.evictor != null) {
			env.evictor.requestShutdown();
		}
	}
	
	@Pointcut("execution(com.sleepycat.je.recovery.Checkpointer.CheckpointStartResult com.sleepycat.je.recovery.Checkpointer.hook_checkpointStart(String, boolean, boolean)) && this(cp)")
	public void  hook_checkpointStart(Checkpointer cp) {}
	
	@Around("hook_checkpointStart(cp)")
	public CheckpointStartResult around1(Checkpointer cp, ProceedingJoinPoint pjp) throws Throwable {
		synchronized (cp.envImpl.getEvictor()) {
			Object[] objs = new Object[] {cp};
			return (CheckpointStartResult)pjp.proceed(objs);
		}
	}
	
	@Pointcut("((call(void com.sleepycat.je.recovery.RecoveryManager.rebuildINList()) && withincode(void com.sleepycat.je.recovery.RecoveryManager.buildTree())) || (call(void hook_invokeEvictor()) && within(com.sleepycat.je.recovery.RecoveryManager))) && this(recoveryManager)")
	public void rebuildINListOrhook_invokeEvictor(RecoveryManager recoveryManager) {}
	
	@After("rebuildINListOrhook_invokeEvictor(recoveryManager)")
	public void after2(RecoveryManager recoveryManager) throws DatabaseException {
		recoveryManager.env.invokeEvictor();
	}
	
	@Pointcut("execution(void com.sleepycat.je.cleaner.UtilizationProfile.hook_evictCursor(com.sleepycat.je.dbi.CursorImpl)) && args(cursor)")
	public void hook_evictCursor(CursorImpl cursor) {} 
	
	@After("hook_evictCursor(cursor)")
	public void after2(CursorImpl cursor) throws DatabaseException {
		cursor.evict();
	}
}
