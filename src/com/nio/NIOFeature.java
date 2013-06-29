package com.nio;

import java.io.File;

import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.FileManager;

@Aspect
public class NIOFeature {

	@Pointcut("call(com.sleepycat.je.log.FileManager.new(com.sleepycat.je.dbi.EnvironmentImpl, java.io.File, boolean)) && args(envImpl, dbEnvHome, readOnly)")
	public void fileManagerConstructor(EnvironmentImpl envImpl, File dbEnvHome, boolean readOnly) {}
	
	@Around("fileManagerConstructor(envImpl, dbEnvHome, readOnly)")
	public FileManager around1(EnvironmentImpl envImpl, File dbEnvHome,
			boolean readOnly) throws DatabaseException {
			return new NIOFileManager(envImpl, dbEnvHome, readOnly);
	}
}