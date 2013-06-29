package com.sleepycat.je.log;

import java.nio.ByteBuffer;

import com.sleepycat.je.DatabaseException;

public class EntryHeader {
	
	private byte loggableType;
	private byte version;
	private long prevOffset;
	private int entrySize;

	/**
	 * Read the log entry header
	 */
	public void readHeader(ByteBuffer dataBuffer) throws DatabaseException {
		readHeader(dataBuffer,false);
	}
	
	public void readHeader(ByteBuffer dataBuffer, boolean skipPrevOffset) throws DatabaseException {

		/* Read the log entry header. */
		loggableType = dataBuffer.get();


		version = dataBuffer.get();
		if (!skipPrevOffset)
			prevOffset = LogUtils.getUnsignedInt(dataBuffer);
		else 
			dataBuffer.position(dataBuffer.position()+LogManager.PREV_BYTES);
		entrySize = LogUtils.readInt(dataBuffer);
	}


	public byte getLoggableType (){
		return loggableType;
	}
	public byte getVersion(){
		return version;
	}
	public long getPrevOffset(){
		return prevOffset;
	}
	public int getEntrySize(){
		return entrySize;
	}
}
