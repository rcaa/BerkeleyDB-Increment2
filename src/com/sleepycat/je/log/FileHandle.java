/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002-2006
 *      Sleepycat Software.  All rights reserved.
 *
 * $Id: FileHandle.java,v 1.1.6.1.2.2 2006/08/29 10:17:45 ckaestne Exp $
 */

package com.sleepycat.je.log;

import java.io.IOException;
import java.io.RandomAccessFile;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.EnvironmentImpl;

/**
 * A FileHandle embodies a File and its accompanying latch.
 */
class FileHandle {
	private RandomAccessFile file;


	private boolean oldHeaderVersion;

	FileHandle(RandomAccessFile file, String fileName, EnvironmentImpl env,
			boolean oldHeaderVersion) {
		this.file = file;
		this.oldHeaderVersion = oldHeaderVersion;
	}

	RandomAccessFile getFile() {
		return file;
	}

	boolean isOldHeaderVersion() {
		return oldHeaderVersion;
	}


	void release() throws DatabaseException {
	}

	void close() throws IOException {

		if (file != null) {
			file.close();
			file = null;
		}
	}
}
