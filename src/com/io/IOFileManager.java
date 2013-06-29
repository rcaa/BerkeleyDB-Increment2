package com.io;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.FileManager;

public class IOFileManager extends FileManager {

	public IOFileManager(EnvironmentImpl envImpl, File dbEnvHome,
			boolean readOnly) throws DatabaseException {

		super(envImpl, dbEnvHome, readOnly);
	}

	void readFromFile(RandomAccessFile file, ByteBuffer readBuffer, long offset)
			throws IOException {

		/*
		 * Perform a RandomAccessFile read and update the buffer position.
		 * ByteBuffer.array() is safe to use since all non-direct ByteBuffers
		 * have a backing array. 
		 */
			assert readBuffer.hasArray();
			assert readBuffer.arrayOffset() == 0;

			int pos = readBuffer.position();
			int size = readBuffer.limit() - pos;
			file.seek(offset);
			int bytesRead = file.read(readBuffer.array(), pos, size);
			if (bytesRead > 0) {
				readBuffer.position(pos + bytesRead);
			}
	}

	protected int writeToFile(RandomAccessFile file, ByteBuffer data,
			long destOffset) throws IOException, DatabaseException {

		int totalBytesWritten = 0;
		/*
		 * Perform a RandomAccessFile write and update the buffer position.
		 * ByteBuffer.array() is safe to use since all non-direct ByteBuffers
		 * have a backing array. 
		 */
			assert data.hasArray();
			assert data.arrayOffset() == 0;

			int pos = data.position();
			int size = data.limit() - pos;
			file.seek(destOffset);
			file.write(data.array(), pos, size);
			data.position(pos + size);
			totalBytesWritten = size;

		return totalBytesWritten;
	}
}