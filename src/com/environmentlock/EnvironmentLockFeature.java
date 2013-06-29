package com.environmentlock;

import java.io.File;
import java.io.IOException;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.cleaner.Cleaner;
import com.sleepycat.je.log.FileManager;

@Aspect
public class EnvironmentLockFeature {

	@Pointcut("execution(boolean com.sleepycat.je.cleaner.Cleaner.hook_lockEnvironment()) && this(c)")
	public void hook_lockEnvironment(Cleaner c) {}
	
	@Around("hook_lockEnvironment(c)")
	public boolean around1(Cleaner c, ProceedingJoinPoint pjp) throws Throwable {
		Object[] objs = new Object[] {c};
		if (!(Boolean)pjp.proceed(objs))
			return false;
		/*
		 * If we can't get an exclusive lock, then there are reader processes
		 * and we can't delete any cleaned files.
		 */
		if (!c.env.getFileManager().lockEnvironment(false, true)) {
			// refined trace Tracer.trace(Level.SEVERE, env, "Cleaner has "
			// + safeFiles.size() +" files not deleted because of read-only
			// processes.");
			return false;
		}
		return true;
	}
	
	@Pointcut("execution(com.sleepycat.je.log.FileManager.new(com.sleepycat.je.dbi.EnvironmentImpl,java.io.File,boolean)) && args(com.sleepycat.je.dbi.EnvironmentImpl, dbEnvHome,readOnly) && this(fm)")
	public void fileManagerConstructor(boolean readOnly, FileManager fm, File dbEnvHome) {}
	
	@Before("fileManagerConstructor(readOnly, fm, dbEnvHome)")
	public void before1(boolean readOnly, FileManager fm, File dbEnvHome) throws DatabaseException {
		fm.dbEnvHome = dbEnvHome;
		fm.lockEnvironment(readOnly, false);
	}
	
	@Pointcut("execution(void com.sleepycat.je.cleaner.Cleaner.hook_releaseEnvironment()) && this(c)")
	public void hook_releaseEnvironment(Cleaner c) {}
	
	@Before("hook_releaseEnvironment(c)")
	public void before2(Cleaner c) throws DatabaseException {
		c.env.getFileManager().releaseExclusiveLock();
	}
	
	@Pointcut("execution(void com.sleepycat.je.log.FileManager.close()) && this(fm)")
	public void close(FileManager fm) {}
	
	@After("close(fm)")
	public void after1(FileManager fm) throws IOException, DatabaseException {
		if (fm.exclLock != null) {
			fm.exclLock.release();
		}
		if (fm.lockFile != null) {
			fm.lockFile.close();
		}
		if (fm.channel != null) {
			fm.channel.close();
		}
		if (fm.envLock != null) {
			fm.envLock.release();
		}
	}
}
