package com.nio;

import java.io.File;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.dbi.EnvironmentImpl;

public aspect NIOAbstract {

	pointcut fileManagerConstructor(EnvironmentImpl envImpl, File dbEnvHome,
			boolean readOnly) 
	: call(FileManager.new(EnvironmentImpl, File, boolean)) && args(envImpl, dbEnvHome, readOnly);
	
	FileManager around(EnvironmentImpl envImpl, File dbEnvHome,
			boolean readOnly) throws DatabaseException :
				fileManagerConstructor(envImpl, dbEnvHome, readOnly) {
			return new NIOFileManager(envImpl, dbEnvHome, readOnly);
	}
}
