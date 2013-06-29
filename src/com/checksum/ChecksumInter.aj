package com.checksum;

import java.nio.ByteBuffer;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.log.EntryHeader;
import com.sleepycat.je.log.FileReader;
import com.sleepycat.je.log.LogManager;

public privileged aspect ChecksumInter {

	/**
	 * add properties to the file reader and initialize the checksum object
	 */
	/* For checking checksum on the read. */
	// TODO made public because protected intertype declarations are not allowed
	public ChecksumValidator FileReader.cksumValidator;

	//changed to public
	public boolean FileReader.doValidateChecksum; // Validate checksums

	// changed to public
	public boolean FileReader.alwaysValidateChecksum; // Validate for all

	// entry types

	/* True if this is the scavenger and we are expecting checksum issues. */
	// TODO made public because protected intertype declarations are not allowed
	public boolean FileReader.anticipateChecksumErrors;

	/**
	 * Reset the checksum and add the header bytes. This method must be called
	 * with the entry header data at the buffer mark.
	 */
	//changed to public
	public void FileReader.startChecksum(ByteBuffer dataBuffer)
			throws DatabaseException {

		/* Move back up to the beginning of the cksum covered header. */
		cksumValidator.reset();
		int entryStart = threadSafeBufferPosition(dataBuffer);
		dataBuffer.reset();
		cksumValidator.update(env, dataBuffer, LogManager.HEADER_CONTENT_BYTES,
				anticipateChecksumErrors);

		/* Move the data buffer back to where the log entry starts. */
		threadSafeBufferPosition(dataBuffer, entryStart);
	}

	public long FileReader.currentEntryChecksum;

	/**
	 * Add the entry bytes to the checksum and check the value. This method must
	 * be called with the buffer positioned at the start of the entry.
	 */
	private void FileReader.validateChecksum(ByteBuffer entryBuffer)
			throws DatabaseException {

		cksumValidator.update(env, entryBuffer, currentEntrySize,
				anticipateChecksumErrors);
		cksumValidator.validate(env, currentEntryChecksum, readBufferFileNum,
				currentEntryOffset, anticipateChecksumErrors);
	}

	/**
	 * Whether to always validate the checksum, even for non-target entries.
	 */
	public void FileReader.setAlwaysValidateChecksum(boolean validate) {
		alwaysValidateChecksum = validate;
	}

	// changed to public
	public boolean LogManager.doChecksumOnRead; // if true, do checksum on

	// read

	public boolean LogManager.getChecksumOnRead() {
		return doChecksumOnRead;
	}

	// changed to public
	public long EntryHeader.checksum;

	public long EntryHeader.getChecksum() {
		return checksum;
	}

	static final int LogManager.CHECKSUM_BYTES = 4; // size of checksum field

	static final int LogManager.HEADER_CHECKSUM_OFFSET = 0; // size of checksum

}
