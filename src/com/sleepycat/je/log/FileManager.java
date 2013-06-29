/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002-2006
 *      Sleepycat Software.  All rights reserved.
 *
 * $Id: FileManager.java,v 1.1.6.1.2.10 2007/03/01 23:15:12 ckaestne Exp $
 */

package com.sleepycat.je.log;



import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.RunRecoveryException;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.entry.LogEntry;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.HexFormatter;

/**
 * The FileManager presents the abstraction of one contiguous file. It doles out
 * LSNs.
 */
public class FileManager {

	public static class FileMode {
		public static final FileMode READ_MODE = new FileMode("r");

		public static final FileMode READWRITE_MODE = new FileMode("rw");

		private String fileModeValue;

		private FileMode(String fileModeValue) {
			this.fileModeValue = fileModeValue;
		}

		public String getModeValue() {
			return fileModeValue;
		}
	}

	private static final String DEBUG_NAME = FileManager.class.getName();

	/* The number of writes that have been performed. */
	private static long writeCount = 0;

	private static long stopOnWriteCount = Long.MAX_VALUE;

	public static final String JE_SUFFIX = ".jdb"; // regular log files

	public static final String CIF_SUFFIX = ".cif"; // cleaner info files

	public static final String DEL_SUFFIX = ".del"; // cleaned files

	public static final String BAD_SUFFIX = ".bad"; // corrupt files

	public static final String LOCK_SUFFIX = ".lck"; // lock files

	static final String[] DEL_SUFFIXES = { DEL_SUFFIX };

	static final String[] JE_SUFFIXES = { JE_SUFFIX };

	private static final String[] JE_AND_DEL_SUFFIXES = { JE_SUFFIX, DEL_SUFFIX };

	/* May be set to false to speed unit tests. */
	private boolean syncAtFileEnd = true;

	// changed to public
	public EnvironmentImpl envImpl;

	private long maxFileSize;

	// changed to public
	public File dbEnvHome;

	/* True if .del files should be included in the list of log files. */
	private boolean includeDeletedFiles = false;





	/* True if all files should be opened readonly. */
	private boolean readOnly;

	/* Handles onto log position */
	private long currentFileNum; // number of the current file

	private long nextAvailableLsn; // nextLSN is the next one available

	private long lastUsedLsn; // last LSN used in the current log file

	private long prevOffset; // Offset to use for the previous pointer

	private boolean forceNewFile; // Force new file on next write

	/*
	 * Saved versions of above. Save this in case a write causes an IOException,
	 * we can back the log up to the last known good LSN.
	 */
	private long savedCurrentFileNum;

	private long savedNextAvailableLsn; // nextLSN is the next one available

	private long savedLastUsedLsn; // last LSN used in the current log file

	private long savedPrevOffset; // Offset to use for the previous pointer

	private boolean savedForceNewFile;

	/* endOfLog is used for writes and fsyncs to the end of the log. */
	private LogEndFileDescriptor endOfLog;


	/*
	 * When we bump the LSNs over to a new file, we must remember the last LSN
	 * of the previous file so we can set the prevOffset field of the file
	 * header appropriately. We have to save it in a map because there's a time
	 * lag between when we know what the last LSN is and when we actually do the
	 * file write, because LSN bumping is done before we get a write buffer.
	 * This map is keyed by file num->last LSN.
	 */
	private Map perFileLastUsedLsn;


	/**
	 * Set up the file cache and initialize the file manager to point to the
	 * beginning of the log.
	 * 
	 * @param configManager
	 * @param dbEnvHome
	 *            environment home directory
	 */
	public FileManager(EnvironmentImpl envImpl, File dbEnvHome, boolean readOnly)
			throws DatabaseException {

		this.envImpl = envImpl;
		this.dbEnvHome = dbEnvHome;
		this.readOnly = readOnly;

		/* Read configurations. */
		DbConfigManager configManager = envImpl.getConfigManager();
		maxFileSize = configManager.getLong(EnvironmentParams.LOG_FILE_MAX);

		if (!dbEnvHome.exists()) {
			throw new LogException("Environment home " + dbEnvHome
					+ " doesn't exist");
		}

		/* Start out as if no log existed. */
		currentFileNum = 0L;
		nextAvailableLsn = DbLsn.makeLsn(currentFileNum, firstLogEntryOffset());
		lastUsedLsn = DbLsn.NULL_LSN;
		perFileLastUsedLsn = new HashMap();
		prevOffset = 0L;
		endOfLog = new LogEndFileDescriptor();
		forceNewFile = false;
		saveLastPosition();

		String stopOnWriteProp = System.getProperty("je.debug.stopOnWrite");
		if (stopOnWriteProp != null) {
			stopOnWriteCount = Long.parseLong(stopOnWriteProp);
		}

	}

	/**
	 * Set the file manager's "end of log".
	 * 
	 * @param nextAvailableLsn
	 *            LSN to be used for the next log entry
	 * @param lastUsedLsn
	 *            last LSN to have a valid entry, may be null
	 * @param prevOffset
	 *            value to use for the prevOffset of the next entry. If the
	 *            beginning of the file, this is 0.
	 */
	public void setLastPosition(long nextAvailableLsn, long lastUsedLsn,
			long prevOffset) {
		this.lastUsedLsn = lastUsedLsn;
		perFileLastUsedLsn.put(new Long(DbLsn.getFileNumber(lastUsedLsn)),
				new Long(lastUsedLsn));
		this.nextAvailableLsn = nextAvailableLsn;
		currentFileNum = DbLsn.getFileNumber(this.nextAvailableLsn);
		this.prevOffset = prevOffset;
		saveLastPosition();
	}

	/*
	 * Cause the current LSN state to be saved in case we fail after we have
	 * bumped the lsn pointer but before we've successfully marshalled into the
	 * log buffer.
	 */
	void saveLastPosition() {
		savedNextAvailableLsn = nextAvailableLsn;
		savedLastUsedLsn = lastUsedLsn;
		savedPrevOffset = prevOffset;
		savedForceNewFile = forceNewFile;
		savedCurrentFileNum = currentFileNum;
	}

	void restoreLastPosition() {
		nextAvailableLsn = savedNextAvailableLsn;
		lastUsedLsn = savedLastUsedLsn;
		prevOffset = savedPrevOffset;
		forceNewFile = savedForceNewFile;
		currentFileNum = savedCurrentFileNum;
	}

	/**
	 * May be used to disable sync at file end to speed unit tests. Must only be
	 * used for unit testing, since log corruption may result.
	 */
	public void setSyncAtFileEnd(boolean sync) {
		syncAtFileEnd = sync;
	}

	/*
	 * File management
	 */

	/**
	 * public for cleaner.
	 * 
	 * @return the number of the first file in this environment.
	 */
	public Long getFirstFileNum() {
		return getFileNum(true);
	}

	public boolean getReadOnly() {
		return readOnly;
	}

	/**
	 * @return the number of the last file in this environment.
	 */
	public Long getLastFileNum() {
		return getFileNum(false);
	}

	/*
	 * For unit tests.
	 */
	public long getCurrentFileNum() {
		return currentFileNum;
	}

	public void setIncludeDeletedFiles(boolean includeDeletedFiles) {
		this.includeDeletedFiles = includeDeletedFiles;
	}

	/**
	 * Get all JE file numbers.
	 * 
	 * @return an array of all JE file numbers.
	 */
	public Long[] getAllFileNumbers() {
		/* Get all the names in sorted order. */
		String[] names = listFiles(JE_SUFFIXES);
		Long[] nums = new Long[names.length];
		for (int i = 0; i < nums.length; i += 1) {
			nums[i] = getNumFromName(names[i]);
		}
		return nums;
	}

	/**
	 * Get the next file number before/after currentFileNum.
	 * 
	 * @param currentFileNum
	 *            the file we're at right now. Note that it may not exist, if
	 *            it's been cleaned and renamed.
	 * @param forward
	 *            if true, we want the next larger file, if false we want the
	 *            previous file
	 * @return null if there is no following file, or if filenum doesn't exist
	 */
	public Long getFollowingFileNum(long currentFileNum, boolean forward) {
		/* Get all the names in sorted order. */
		String[] names = listFiles(JE_SUFFIXES);

		/* Search for the current file. */
		String searchName = getFileName(currentFileNum, JE_SUFFIX);
		int foundIdx = Arrays.binarySearch(names, searchName);

		boolean foundTarget = false;
		if (foundIdx >= 0) {
			if (forward) {
				foundIdx++;
			} else {
				foundIdx--;
			}
		} else {

			/*
			 * currentFileNum not found (might have been cleaned). FoundIdx will
			 * be (-insertionPoint - 1).
			 */
			foundIdx = Math.abs(foundIdx + 1);
			if (!forward) {
				foundIdx--;
			}
		}

		/* The current fileNum is found, return the next or prev file. */
		if (forward && (foundIdx < names.length)) {
			foundTarget = true;
		} else if (!forward && (foundIdx > -1)) {
			foundTarget = true;
		}

		if (foundTarget) {
			return getNumFromName(names[foundIdx]);
		} else {
			return null;
		}
	}

	/**
	 * @return true if there are any files at all.
	 */
	public boolean filesExist() {
		String[] names = listFiles(JE_SUFFIXES);
		return (names.length != 0);
	}

	/**
	 * Get the first or last file number in the set of je files.
	 * 
	 * @param first
	 *            if true, get the first file, else get the last file
	 * @return the file number or null if no files exist
	 */
	private Long getFileNum(boolean first) {
		String[] names = listFiles(JE_SUFFIXES);
		if (names.length == 0) {
			return null;
		} else {
			int index = 0;
			if (!first) {
				index = names.length - 1;
			}
			return getNumFromName(names[index]);
		}
	}

	/**
	 * Get the file number from a file name.
	 * 
	 * @param the
	 *            file name
	 * @return the file number
	 */
	private Long getNumFromName(String fileName) {
		String fileNumber = fileName.substring(0, fileName.indexOf("."));
		return new Long(Long.parseLong(fileNumber, 16));
	}

	/**
	 * Find je files. Return names sorted in ascending fashion.
	 * 
	 * @param suffix
	 *            which type of file we're looking for
	 * @return array of file names
	 */
	String[] listFiles(String[] suffixes) {
		String[] fileNames = dbEnvHome.list(new JEFileFilter(suffixes));
		Arrays.sort(fileNames);
		return fileNames;
	}

	/**
	 * Find je files, flavor for unit test support.
	 * 
	 * @param suffix
	 *            which type of file we're looking for
	 * @return array of file names
	 */
	public static String[] listFiles(File envDirFile, String[] suffixes) {
		String[] fileNames = envDirFile.list(new JEFileFilter(suffixes));
		Arrays.sort(fileNames);
		return fileNames;
	}

	/**
	 * @return the full file name and path for the nth je file.
	 */
	String[] getFullFileNames(long fileNum) {
		if (includeDeletedFiles) {
			int nSuffixes = JE_AND_DEL_SUFFIXES.length;
			String[] ret = new String[nSuffixes];
			for (int i = 0; i < nSuffixes; i++) {
				ret[i] = getFullFileName(getFileName(fileNum,
						JE_AND_DEL_SUFFIXES[i]));
			}
			return ret;
		} else {
			return new String[] { getFullFileName(getFileName(fileNum,
					JE_SUFFIX)) };
		}
	}

	/**
	 * @return the full file name and path for the given file number and suffix.
	 */
	public String getFullFileName(long fileNum, String suffix) {
		return getFullFileName(getFileName(fileNum, suffix));
	}

	/**
	 * @return the full file name and path for this file name.
	 */
	private String getFullFileName(String fileName) {
		return dbEnvHome + File.separator + fileName;
	}

	/**
	 * @return the file name for the nth file.
	 */
	public static String getFileName(long fileNum, String suffix) {

		/*
		 * HexFormatter generates a 0 padded string starting with 0x. We want
		 * the right most 8 digits, so start at 10.
		 */
		return (HexFormatter.formatLong(fileNum).substring(10) + suffix);
	}

	/**
	 * Rename this file to NNNNNNNN.suffix. If that file already exists, try
	 * NNNNNNNN.suffix.1, etc. Used for deleting files or moving corrupt files
	 * aside.
	 * 
	 * @param fileNum
	 *            the file we want to move
	 * @param newSuffix
	 *            the new file suffix
	 */
	public void renameFile(long fileNum, String newSuffix)
			throws DatabaseException, IOException {

		int repeatNum = 0;
		boolean renamed = false;
		while (!renamed) {
			String generation = "";
			if (repeatNum > 0) {
				generation = "." + repeatNum;
			}
			String newName = getFullFileName(getFileName(fileNum, newSuffix)
					+ generation);
			File targetFile = new File(newName);
			if (targetFile.exists()) {
				repeatNum++;
			} else {
				String oldFileName = getFullFileNames(fileNum)[0];
				File oldFile = new File(oldFileName);
				if (oldFile.renameTo(targetFile)) {
					renamed = true;
				} else {
					throw new LogException("Couldn't rename " + oldFileName
							+ " to " + newName);
				}
			}
		}
	}

	/**
	 * Delete log file NNNNNNNN.
	 * 
	 * @param fileNum
	 *            the file we want to move
	 */
	public void deleteFile(long fileNum) throws DatabaseException, IOException {

		String fileName = getFullFileNames(fileNum)[0];
		File file = new File(fileName);
		boolean done = file.delete();
		if (!done) {
			throw new LogException("Couldn't delete " + file);
		}
	}

	//FEATURE remove the complete persistency layer, so that it can be replaced by something else
	
	/**
	 * Return a read only file handle that corresponds the this file number.
	 * Retrieve it from the cache or open it anew and validate the file header.
	 * This method takes a latch on this file, so that the file descriptor will
	 * be held in the cache as long as it's in use. When the user is done with
	 * the file, the latch must be released.
	 * 
	 * @param fileNum
	 *            which file
	 * @return the file handle for the existing or newly created file
	 */
	FileHandle getFileHandle(long fileNum) throws LogException,
			DatabaseException {

		return makeFileHandle(fileNum, FileMode.READ_MODE);
	}

	private FileHandle makeFileHandle(long fileNum, FileMode mode)
			throws DatabaseException {

		String[] fileNames = getFullFileNames(fileNum);
		RandomAccessFile newFile = null;
		String fileName = null;
		try {

			/*
			 * Open the file. Note that we are going to try a few names to open
			 * this file -- we'll try for N.jdb, and if that doesn't exist and
			 * we're configured to look for all types, we'll look for N.del.
			 */
			FileNotFoundException FNFE = null;
			for (int i = 0; i < fileNames.length; i++) {
				fileName = fileNames[i];
				try {
					newFile = new RandomAccessFile(fileName, mode
							.getModeValue());
					break;
				} catch (FileNotFoundException e) {
					/* Save the first exception thrown. */
					if (FNFE == null) {
						FNFE = e;
					}
				}
			}

			/*
			 * If we didn't find the file or couldn't create it, rethrow the
			 * exception.
			 */
			if (newFile == null) {
				throw FNFE;
			}

			boolean oldHeaderVersion = false;

			if (newFile.length() == 0) {
				/*
				 * If the file is empty, reinitialize it if we can. If not, send
				 * the file handle back up; the calling code will deal with the
				 * fact that there's nothing there.
				 */
				if (mode == FileMode.READWRITE_MODE) {
					/* An empty file, write a header. */
					long lastLsn = DbLsn.longToLsn((Long) perFileLastUsedLsn
							.remove(new Long(fileNum - 1)));
					long headerPrevOffset = 0;
					if (lastLsn != DbLsn.NULL_LSN) {
						headerPrevOffset = DbLsn.getFileOffset(lastLsn);
					}
					FileHeader fileHeader = new FileHeader(fileNum,
							headerPrevOffset);
					writeFileHeader(newFile, fileName, fileHeader);
				}
			} else {
				/* A non-empty file, check the header */
				oldHeaderVersion = readAndValidateFileHeader(newFile, fileName,
						fileNum);
			}
			return new FileHandle(newFile, fileName, envImpl, oldHeaderVersion);
		} catch (FileNotFoundException e) {
			throw new LogFileNotFoundException("Couldn't open file " + fileName
					+ ": " + e.getMessage());
	
		} catch (Throwable t) {

			/*
			 * Catch Throwable here (rather than exception) because in unit test
			 * mode, we run assertions and they throw errors. We want to clean
			 * up the file object in all cases.
			 */
			closeFileInErrorCase(newFile);
			throw new DatabaseException("Couldn't open file " + fileName + ": "
					+ t, t);
		}
	}

	/**
	 * Close this file and eat any exceptions. Used in catch clauses.
	 */
	// changed to public
	public void closeFileInErrorCase(RandomAccessFile file) {
		try {
			if (file != null) {
				file.close();
			}
		} catch (IOException e) {

			/*
			 * Klockwork - ok Couldn't close file, oh well.
			 */
		}
	}

	/**
	 * Read the given je log file and validate the header.
	 * 
	 * @throws DatabaseException
	 *             if the file header isn't valid
	 * 
	 * @return whether the file header has an old version number.
	 */
	private boolean readAndValidateFileHeader(RandomAccessFile file,
			String fileName, long fileNum) throws DatabaseException,
			IOException {

		/*
		 * Read the file header from this file. It's always the first log entry.
		 */
		LogManager logManager = envImpl.getLogManager();
		LogEntry headerEntry = logManager.getLogEntry(
				DbLsn.makeLsn(fileNum, 0), file);
		FileHeader header = (FileHeader) headerEntry.getMainItem();
		return header.validate(fileName, fileNum);
	}

	/**
	 * Write a proper file header to the given file.
	 */
	private void writeFileHeader(RandomAccessFile file, String fileName,
			FileHeader header) throws DatabaseException, IOException {

		/*
		 * Fail loudly if the environment is invalid. A RunRecoveryException
		 * must have occurred.
		 */
		envImpl.checkIfInvalid();

		/*
		 * Fail silent if the environment is not open.
		 */
		if (envImpl.mayNotWrite()) {
			return;
		}

		/* Serialize the header into this buffer. */
		int headerSize = header.getLogSize();
		int entrySize = headerSize + LogManager.HEADER_BYTES;
		ByteBuffer headerBuf = envImpl.getLogManager().putIntoBuffer(header,
				headerSize, 0, false, entrySize);

		if (++writeCount >= stopOnWriteCount) {
			Runtime.getRuntime().halt(0xff);
		}

		/* Write the buffer into the channel. */
		int bytesWritten;
		try {
			if (RUNRECOVERY_EXCEPTION_TESTING) {
				generateRunRecoveryException(file, headerBuf, 0);
			}
			bytesWritten = writeToFile(file, headerBuf, 0);
		} catch (ClosedChannelException e) {

			/*
			 * The channel should never be closed. It may be closed because of
			 * an interrupt received by another thread. See SR [#10463]
			 */
			throw new RunRecoveryException(envImpl,
					"Channel closed, may be due to thread interrupt", e);
		} catch (IOException e) {
			/* Possibly an out of disk exception. */
			throw new RunRecoveryException(envImpl, "IOException caught: " + e);
		}

		if (bytesWritten != entrySize) {
			throw new LogException("File " + fileName
					+ " was created with an incomplete header. Only "
					+ bytesWritten + " bytes were written.");
		}
	}

	/**
	 * @return the prevOffset field stored in the file header.
	 */
	long getFileHeaderPrevOffset(long fileNum) throws IOException,
			DatabaseException {

		LogEntry headerEntry = envImpl.getLogManager().getLogEntry(
				DbLsn.makeLsn(fileNum, 0));
		FileHeader header = (FileHeader) headerEntry.getMainItem();
		return header.getLastEntryInPrevFileOffset();
	}

	/*
	 * Support for writing new log entries
	 */

	/**
	 * @return the file offset of the last LSN that was used. For constructing
	 *         the headers of log entries. If the last LSN that was used was in
	 *         a previous file, or this is the very first LSN of the whole
	 *         system, return 0.
	 */
	long getPrevEntryOffset() {
		return prevOffset;
	}

	/**
	 * Increase the current log position by "size" bytes. Move the prevOffset
	 * pointer along.
	 * 
	 * @param size
	 *            is an unsigned int
	 * @return true if we flipped to the next log file.
	 */
	boolean bumpLsn(long size) {

		/* Save copy of initial lsn state. */
		saveLastPosition();

		boolean flippedFiles = false;

		if (forceNewFile
				|| (DbLsn.getFileOffset(nextAvailableLsn) + size) > maxFileSize) {

			forceNewFile = false;

			/* Move to another file. */
			currentFileNum++;

			/* Remember the last used LSN of the previous file. */
			if (lastUsedLsn != DbLsn.NULL_LSN) {
				perFileLastUsedLsn.put(new Long(DbLsn
						.getFileNumber(lastUsedLsn)), new Long(lastUsedLsn));
			}
			prevOffset = 0;
			lastUsedLsn = DbLsn.makeLsn(currentFileNum, firstLogEntryOffset());
			flippedFiles = true;
		} else {
			if (lastUsedLsn == DbLsn.NULL_LSN) {
				prevOffset = 0;
			} else {
				prevOffset = DbLsn.getFileOffset(lastUsedLsn);
			}
			lastUsedLsn = nextAvailableLsn;
		}
		nextAvailableLsn = DbLsn.makeLsn(DbLsn.getFileNumber(lastUsedLsn),
				(DbLsn.getFileOffset(lastUsedLsn) + size));

		return flippedFiles;
	}
	static boolean IO_EXCEPTION_TESTING = false;

	/**
	 * Write out a log buffer to the file.
	 * 
	 * @param fullBuffer
	 *            buffer to write
	 */
	void writeLogBuffer(LogBuffer fullBuffer) throws DatabaseException {

		/*
		 * Fail loudly if the environment is invalid. A RunRecoveryException
		 * must have occurred.
		 */
		envImpl.checkIfInvalid();

		/*
		 * Fail silent if the environment is not open.
		 */
		//FEATURE this check belongs to a feature of the recovery manager?
		if (envImpl.mayNotWrite()) {
			return;
		}

		/* Use the LSN to figure out what file to write this buffer to. */
		long firstLsn = fullBuffer.getFirstLsn();

		/*
		 * Is there anything in this write buffer? We could have been called by
		 * the environment shutdown, and nothing is actually in the buffer.
		 */
		if (firstLsn != DbLsn.NULL_LSN) {

			RandomAccessFile file = endOfLog.getWritableFile(DbLsn
					.getFileNumber(firstLsn));
			ByteBuffer data = fullBuffer.getDataBuffer();

			if (++writeCount >= stopOnWriteCount) {
				Runtime.getRuntime().halt(0xff);
			}

			try {



				if (IO_EXCEPTION_TESTING) {
					throw new IOException("generated for testing");
				}
				if (RUNRECOVERY_EXCEPTION_TESTING) {
					generateRunRecoveryException(file, data, DbLsn
							.getFileOffset(firstLsn));
				}
				writeToFile(file, data, DbLsn.getFileOffset(firstLsn));
			} catch (ClosedChannelException e) {

				/*
				 * The file should never be closed. It may be closed because of
				 * an interrupt received by another thread. See SR [#10463].
				 */
				throw new RunRecoveryException(envImpl,
						"File closed, may be due to thread interrupt", e);
			} catch (IOException IOE) {
				abortCommittedTxns(data);
				
				throw new DatabaseException(IOE);
			}

			assert EnvironmentImpl.maybeForceYield();
		}
	}

	/**
	 * Write a buffer to a file at a given offset, using NIO if so configured.
	 */
	protected int writeToFile(RandomAccessFile file, ByteBuffer data,
			long destOffset) throws IOException, DatabaseException {
		
		//DUMMY, must be implemented by one of the I/O features

		throw new UnsupportedOperationException();
	}

	/**
	 * Read a buffer from a file at a given offset, using NIO if so configured.
	 */
	void readFromFile(RandomAccessFile file, ByteBuffer readBuffer, long offset)
			throws IOException{

		//DUMMY, must be implemented by one of the I/O features
		throw new UnsupportedOperationException();
	}
	

	/*
	 * Iterate through a buffer looking for commit records. Change all commit
	 * records to abort records.
	 */
	//FEATURE this is only part of the transaction feature
	private void abortCommittedTxns(ByteBuffer data) {
		final byte commitType = LogEntryType.LOG_TXN_COMMIT.getTypeNum();
		final byte abortType = LogEntryType.LOG_TXN_ABORT.getTypeNum();
		data.position(0);

		while (data.remaining() > 0) {
			int recStartPos = data.position();
			data.position(recStartPos + LogManager.HEADER_ENTRY_TYPE_OFFSET);
			int typePos = data.position();
			byte entryType = data.get();
			boolean recomputeChecksum = false;
			if (entryType == commitType) {
				data.position(typePos);
				data.put(abortType);
				recomputeChecksum = true;
			}
			/* Move byte buffer past version. */
			byte version = data.get();
			/* Read the size, skipping over the prev offset. */
			data.position(data.position() + LogManager.PREV_BYTES);
			int itemSize = LogUtils.readInt(data);
			int itemDataStartPos = data.position();
			if (recomputeChecksum) {
				hook_recomputeChecksum(data,recStartPos,itemSize);
			}
			data.position(itemDataStartPos + itemSize);
		}
		data.position(0);
	}

	private void hook_recomputeChecksum(ByteBuffer data, int recStartPos, int itemSize) {
		// TODO Auto-generated method stub
		
	}

	/**
	 * FSync the end of the log.
	 */
	void syncLogEnd() throws DatabaseException {

		try {
			endOfLog.force();
		} catch (IOException e) {
			throw new DatabaseException(e);
		}
	}

	/**
	 * Sync the end of the log, close off this log file. Should only be called
	 * under the log write latch.
	 */
	void syncLogEndAndFinishFile() throws DatabaseException, IOException {

		if (syncAtFileEnd) {
			syncLogEnd();
		}
		endOfLog.close();
	}


	/**
	 * Close all file handles and empty the cache.
	 */
	public void clear() throws IOException, DatabaseException {



		endOfLog.close();
	}

	/**
	 * Clear the file lock.
	 */
	public void close() throws IOException, DatabaseException {






	}



	/**
	 * Ensure that if the environment home dir is on readonly media or in a
	 * readonly directory that the environment has been opened for readonly
	 * access.
	 * 
	 * @return true if the environment home dir is readonly.
	 */
	private boolean checkEnvHomePermissions(boolean readOnly)
			throws DatabaseException {

		boolean envDirIsReadOnly = !dbEnvHome.canWrite();
		if (envDirIsReadOnly && !readOnly) {
			throw new DatabaseException("The Environment directory "
					+ dbEnvHome + " is not writable, but the "
					+ "Environment was opened for read-write access.");
		}

		return envDirIsReadOnly;
	}

	/**
	 * Truncate a log at this position. Used by recovery to a timestamp
	 * utilities and by recovery to set the end-of-log position.
	 * 
	 * <p>
	 * This method forces a new log file to be written next, if the last file
	 * (the file truncated to) has an old version in its header. This ensures
	 * that when the log is opened by an old version of JE, a version
	 * incompatibility will be detected. [#11243]
	 * </p>
	 */
	public void truncateLog(long fileNum, long offset) throws IOException,
			DatabaseException {

		FileHandle handle = makeFileHandle(fileNum, FileMode.READWRITE_MODE);
		RandomAccessFile file = handle.getFile();

		try {
			file.getChannel().truncate(offset);
		} finally {
			file.close();
		}

		if (handle.isOldHeaderVersion()) {
			forceNewFile = true;
		}
	}

	/**
	 * Set the flag that causes a new file to be written before the next write.
	 */
	void forceNewLogFile() {
		forceNewFile = true;
	}

	/**
	 * Return the offset of the first log entry after the file header.
	 */

	/**
	 * @return the size in bytes of the file header log entry.
	 */
	public static int firstLogEntryOffset() {
		return FileHeader.entrySize();// + LogManager.HEADER_BYTES;
	}

	/**
	 * Return the next available LSN in the log. Note that this is
	 * unsynchronized, so is only valid as an approximation of log size.
	 */
	public long getNextLsn() {
		return nextAvailableLsn;
	}

	/**
	 * Return the last allocated LSN in the log. Note that this is
	 * unsynchronized, so if it is called outside the log write latch it is only
	 * valid as an approximation of log size.
	 */
	public long getLastUsedLsn() {
		return lastUsedLsn;
	}



	

	/**
	 * The LogEndFileDescriptor is used to write and fsync the end of the log.
	 * Because the JE log is append only, there is only one logical R/W file
	 * descriptor for the whole environment. This class actually implements two
	 * RandomAccessFile instances, one for writing and one for fsyncing, so the
	 * two types of operations don't block each other.
	 * 
	 * The write file descriptor is considered the master. Manipulation of this
	 * class is done under the log write latch. Here's an explanation of why the
	 * log write latch is sufficient to safeguard all operations.
	 * 
	 * There are two types of callers who may use this file descriptor: the
	 * thread that is currently writing to the end of the log and any threads
	 * that are fsyncing on behalf of the FSyncManager.
	 * 
	 * The writing thread appends data to the file and fsyncs the file when we
	 * flip over to a new log file. The file is only instantiated at the point
	 * that it must do so -- which is either when the first fsync is required by
	 * JE or when the log file is full and we flip files. Therefore, the writing
	 * thread has two actions that change this descriptor -- we initialize the
	 * file descriptor for the given log file at the first write to the file,
	 * and we close the file descriptor when the log file is full. Therefore is
	 * a period when there is no log descriptor -- when we have not yet written
	 * a log buffer into a given log file.
	 * 
	 * The fsyncing threads ask for the log end file descriptor asynchronously,
	 * but will never modify it. These threads may arrive at the point when the
	 * file descriptor is null, and therefore skip their fysnc, but that is fine
	 * because it means a writing thread already flipped that target file and
	 * has moved on to the next file.
	 * 
	 * Time Activity 10 thread 1 writes log entry A into file 0x0, issues fsync
	 * outside of log write latch, yields the processor 20 thread 2 writes log
	 * entry B, piggybacks off thread 1 30 thread 3 writes log entry C, but no
	 * room left in that file, so it flips the log, and fsyncs file 0x0, all
	 * under the log write latch. It nulls out endOfLogRWFile, moves onto file
	 * 0x1, but doesn't create the file yet. 40 thread 1 finally comes along,
	 * but endOfLogRWFile is null-- no need to fsync in that case, 0x0 got
	 * fsynced.
	 */
	class LogEndFileDescriptor {
		private RandomAccessFile endOfLogRWFile = null;

		private RandomAccessFile endOfLogSyncFile = null;

		/**
		 * getWritableFile must be called under the log write latch.
		 */
		RandomAccessFile getWritableFile(long fileNumber)
				throws RunRecoveryException {

			try {

				if (endOfLogRWFile == null) {

					/*
					 * We need to make a file descriptor for the end of the log.
					 * This is guaranteed to be called under the log write
					 * latch.
					 */
					endOfLogRWFile = makeFileHandle(fileNumber,
							FileMode.READWRITE_MODE).getFile();
					endOfLogSyncFile = makeFileHandle(fileNumber,
							FileMode.READWRITE_MODE).getFile();
				}

				return endOfLogRWFile;
			} catch (Exception e) {

				/*
				 * If we can't get a write channel, we need to go into
				 * RunRecovery state.
				 */
				throw new RunRecoveryException(envImpl, e);
			}
		}

		/**
		 * FSync the log file that makes up the end of the log.
		 */
		void force() throws DatabaseException, IOException {

			/*
			 * Get a local copy of the end of the log file descriptor, it could
			 * change. No need to latch, no harm done if we get an old file
			 * descriptor, because we forcibly fsync under the log write latch
			 * when we switch files.
			 * 
			 * If there is no current end file descriptor, we know that the log
			 * file has flipped to a new file since the fsync was issued.
			 */
			RandomAccessFile file = endOfLogSyncFile;
			if (file != null) {

				FileChannel channel = file.getChannel();
				try {
					channel.force(false);
				} catch (ClosedChannelException e) {

					/*
					 * The channel should never be closed. It may be closed
					 * because of an interrupt received by another thread. See
					 * SR [#10463]
					 */
					throw new RunRecoveryException(envImpl,
							"Channel closed, may be due to thread interrupt", e);
				}

				assert EnvironmentImpl.maybeForceYield();
			}
		}

		/**
		 * Close the end of the log file descriptor. Use atomic assignment to
		 * ensure that we won't force and close on the same descriptor.
		 */
		void close() throws IOException {

			IOException firstException = null;
			if (endOfLogRWFile != null) {
				RandomAccessFile file = endOfLogRWFile;

				/*
				 * Null out so that other threads know endOfLogRWFile is no
				 * longer available.
				 */
				endOfLogRWFile = null;
				try {
					file.close();
				} catch (IOException e) {
					/* Save this exception, so we can try the second close. */
					firstException = e;
				}
			}
			if (endOfLogSyncFile != null) {
				RandomAccessFile file = endOfLogSyncFile;

				/*
				 * Null out so that other threads know endOfLogSyncFile is no
				 * longer available.
				 */
				endOfLogSyncFile = null;
				file.close();
			}

			if (firstException != null) {
				throw firstException;
			}
		}
	}

	/*
	 * Generate IOExceptions for testing.
	 */

	/* Testing switch. */
	static boolean RUNRECOVERY_EXCEPTION_TESTING = false;

	/* Max write counter value. */
	private static final int RUNRECOVERY_EXCEPTION_MAX = 100;

	/* Current write counter value. */
	private int runRecoveryExceptionCounter = 0;

	/* Whether an exception has been thrown. */
	private boolean runRecoveryExceptionThrown = false;

	/* Random number generator. */
	private Random runRecoveryExceptionRandom = null;

	private void generateRunRecoveryException(RandomAccessFile file,
			ByteBuffer data, long destOffset) throws DatabaseException,
			IOException {

		if (runRecoveryExceptionThrown) {
			try {
				throw new Exception("Write after RunRecoveryException");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		runRecoveryExceptionCounter += 1;
		if (runRecoveryExceptionCounter >= RUNRECOVERY_EXCEPTION_MAX) {
			runRecoveryExceptionCounter = 0;
		}
		if (runRecoveryExceptionRandom == null) {
			runRecoveryExceptionRandom = new Random(System.currentTimeMillis());
		}
		if (runRecoveryExceptionCounter == runRecoveryExceptionRandom
				.nextInt(RUNRECOVERY_EXCEPTION_MAX)) {
			int len = runRecoveryExceptionRandom.nextInt(data.remaining());
			if (len > 0) {
				byte[] a = new byte[len];
				data.get(a, 0, len);
				ByteBuffer buf = ByteBuffer.wrap(a);
				writeToFile(file, buf, destOffset);
			}
			runRecoveryExceptionThrown = true;
			throw new RunRecoveryException(envImpl,
					"Randomly generated for testing");
		}
	}
}
