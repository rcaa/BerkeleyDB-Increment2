package com.nio;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.FileManager;

public class NIOFileManager extends FileManager {

	public NIOFileManager(EnvironmentImpl envImpl, File dbEnvHome,
			boolean readOnly) throws DatabaseException {
		super(envImpl, dbEnvHome, readOnly);
	}

	void readFromFile(RandomAccessFile file, ByteBuffer readBuffer, long offset)
			throws IOException {

		FileChannel channel = file.getChannel();

		/*
		 * Perform a single read using NIO.
		 */
		channel.read(readBuffer, offset);
	}

	protected int writeToFile(RandomAccessFile file, ByteBuffer data,
			long destOffset) throws IOException, DatabaseException {

		FileChannel channel = file.getChannel();

		/*
		 * Perform a single write using NIO.
		 */
		return channel.write(data, destOffset);

	}
}
