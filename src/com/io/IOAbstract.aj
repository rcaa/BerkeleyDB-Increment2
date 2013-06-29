package com.io;

import java.io.File;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.FileManager;

public aspect IOAbstract {

	pointcut fileManagerConstructor(EnvironmentImpl envImpl, File dbEnvHome,
			boolean readOnly) 
	 : call(FileManager.new(EnvironmentImpl, File, boolean)) && args(envImpl, dbEnvHome, readOnly);

	FileManager around(EnvironmentImpl envImpl, File dbEnvHome, boolean readOnly)
			throws DatabaseException : fileManagerConstructor(envImpl, dbEnvHome, readOnly){
		return new IOFileManager(envImpl, dbEnvHome, readOnly);
	}
}