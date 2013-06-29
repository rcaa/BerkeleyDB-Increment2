/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002-2006
 *      Sleepycat Software.  All rights reserved.
 *
 * $Id: LogBufferPool.java,v 1.1.6.1.2.6 2006/10/25 21:29:05 ckaestne Exp $
 */

package com.sleepycat.je.log;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedList;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.EnvironmentImpl;

/**
 * LogBufferPool keeps a set of log buffers.
 */
//Lck made public
public class LogBufferPool {
	private static final String DEBUG_NAME = LogBufferPool.class.getName();

	private EnvironmentImpl envImpl = null;

	private int logBufferSize; // size of each log buffer

	private LinkedList bufferPool; // List of log buffers

	/* Buffer that holds the current log end. All writes go to this buffer. */
	private LogBuffer currentWriteBuffer;

	private FileManager fileManager;


	private boolean runInMemory;


	LogBufferPool(FileManager fileManager, EnvironmentImpl envImpl)
			throws DatabaseException {

		this.fileManager = fileManager;
		this.envImpl = envImpl;

		/* Configure the pool. */
		DbConfigManager configManager = envImpl.getConfigManager();
		runInMemory = configManager
				.getBoolean(EnvironmentParams.LOG_MEMORY_ONLY);
		reset(configManager);

		/* Current buffer is the active buffer that writes go into. */
		currentWriteBuffer = (LogBuffer) bufferPool.getFirst();
	}

	/**
	 * Initialize the pool at construction time and when the cache is resized.
	 * This method is called after the memory budget has been calculated.
	 */
	void reset(DbConfigManager configManager) throws DatabaseException {

		/*
		 * When running in memory, we can't clear the existing pool and changing
		 * the buffer size is not very useful, so just return.
		 */
		if (runInMemory && bufferPool != null) {
			return;
		}

		/*
		 * Based on the log budget, figure the number and size of log buffers to
		 * use.
		 */
		int numBuffers = configManager
				.getInt(EnvironmentParams.NUM_LOG_BUFFERS);
		long logBufferBudget = envImpl.getLogBufferBudget().getLogBufferBudget();

		/* Buffers must be int sized. */
		int newBufferSize = (int) logBufferBudget / numBuffers;

		/* list of buffers that are available for log writing */
		LinkedList newPool = new LinkedList();

		/*
		 * If we're running in memory only, don't pre-allocate all the buffers.
		 * This case only occurs when called from the constructor.
		 */
		if (runInMemory) {
			numBuffers = 1;
		}

		for (int i = 0; i < numBuffers; i++) {
			newPool.add(new LogBuffer(newBufferSize, envImpl));
		}

		hook_resetPool(newPool,newBufferSize);
	}

	private void hook_resetPool(LinkedList newPool, int newBufferSize) throws DatabaseException {
		bufferPool = newPool;
		logBufferSize = newBufferSize;
	}

	/**
	 * Get a log buffer for writing sizeNeeded bytes. If currentWriteBuffer is
	 * too small or too full, flush currentWriteBuffer and get a new one. Called
	 * within the log write latch.
	 * 
	 * @return a buffer that can hold sizeNeeded bytes.
	 */
	LogBuffer getWriteBuffer(int sizeNeeded, boolean flippedFile)
			throws IOException, DatabaseException {

		/*
		 * We need a new log buffer either because this log buffer is full, or
		 * the LSN has marched along to the next file. Each log buffer only
		 * holds entries that belong to a single file. If we've flipped over
		 * into the next file, we'll need to get a new log buffer even if the
		 * current one has room.
		 */
		if ((!currentWriteBuffer.hasRoom(sizeNeeded)) || flippedFile) {

			/*
			 * Write the currentWriteBuffer to the file and reset
			 * currentWriteBuffer.
			 */
			writeBufferToFile(sizeNeeded);
		}

		if (flippedFile) {
			/* Now that the old buffer has been written to disk, fsync. */
			if (!runInMemory) {
				fileManager.syncLogEndAndFinishFile();
			}
		}

		return currentWriteBuffer;
	}

	/**
	 * Write the contents of the currentWriteBuffer to disk. Leave this buffer
	 * in memory to be available to would be readers. Set up a new
	 * currentWriteBuffer. Assumes the log write latch is held.
	 * 
	 * @param sizeNeeded
	 *            is the size of the next object we need to write to the log.
	 *            May be 0 if this is called on behalf of LogManager.flush().
	 */
	//Lck made public
	public void writeBufferToFile(int sizeNeeded) throws IOException,
			DatabaseException {

		int bufferSize = ((logBufferSize > sizeNeeded) ? logBufferSize
				: sizeNeeded);

		/* We're done with the buffer, flip to make it readable. */
		LogBuffer latchedBuffer = currentWriteBuffer;
		try {
			ByteBuffer currentByteBuffer = currentWriteBuffer.getDataBuffer();
			int savePosition = currentByteBuffer.position();
			int saveLimit = currentByteBuffer.limit();
			currentByteBuffer.flip();

			/* Dispose of it and get a new buffer for writing. */
			if (runInMemory) {
				/* We're done with the current buffer. */
				latchedBuffer.release();
				latchedBuffer = null;
				/* We're supposed to run in-memory, allocate another buffer. */
				hook_allocateBuffer(bufferSize);
			} else {

				/*
				 * If we're configured for writing (not memory-only situation),
				 * write this buffer to disk and find a new buffer to use.
				 */
				try {
					fileManager.writeLogBuffer(currentWriteBuffer);

					/* Rewind so readers can see this. */
					currentWriteBuffer.getDataBuffer().rewind();

					/* We're done with the current buffer. */
					latchedBuffer.release();
					latchedBuffer = null;

					/*
					 * Now look in the linked list for a buffer of the right
					 * size.
					 */
					LogBuffer nextToUse = hook_getNextToUse();
				} catch (DatabaseException DE) {
					currentByteBuffer.position(savePosition);
					currentByteBuffer.limit(saveLimit);
					throw DE;
				}
			}
		} finally {
			if (latchedBuffer != null) {
				latchedBuffer.release();
			}
		}
	}

	private LogBuffer hook_getNextToUse() throws DatabaseException {
		Iterator iter = bufferPool.iterator();
		LogBuffer nextToUse = (LogBuffer) iter.next();

		boolean done = bufferPool.remove(nextToUse);
		assert done;
		nextToUse.reinit();

		/* Put the nextToUse buffer at the end of the queue. */
		bufferPool.add(nextToUse);

		/* Assign currentWriteBuffer with the latch held. */
		currentWriteBuffer = nextToUse;

		return nextToUse;

	}

	private void hook_allocateBuffer(int bufferSize) throws DatabaseException {
		currentWriteBuffer = new LogBuffer(bufferSize, envImpl);
		bufferPool.add(currentWriteBuffer);
	}

	/**
	 * A loggable object has been freshly marshalled into the write log buffer.
	 * 1. Update buffer so it knows what LSNs it contains. 2. If this object
	 * requires a flush, write this buffer out to the backing file. Assumes log
	 * write latch is held.
	 */
	void writeCompleted(long lsn, boolean flushRequired)
			throws DatabaseException, IOException {

		currentWriteBuffer.registerLsn(lsn);
		if (flushRequired) {
			writeBufferToFile(0);
		}
	}

	/**
	 * Find a buffer that holds this LSN.
	 * 
	 * @return the buffer that contains this LSN, latched and ready to read, or
	 *         return null.
	 */
	LogBuffer getReadBuffer(long lsn) throws DatabaseException {
//FEATURE read buffer can be turned off at all
		LogBuffer foundBuffer = null;

			
			Iterator iter = bufferPool.iterator();
			while (iter.hasNext()) {
				LogBuffer l = (LogBuffer) iter.next();
				if (l.containsLsn(lsn)) {
					foundBuffer = l;
					break;
				}
			}

			/*
			 * Check the currentWriteBuffer separately, since if the pool was
			 * recently reset it will not be in the pool.
			 */
			if (foundBuffer == null && currentWriteBuffer.containsLsn(lsn)) {
				foundBuffer = currentWriteBuffer;
			}

			


		if (foundBuffer == null) {
			return null;
		} else {
			return foundBuffer;
		}
	}

}
