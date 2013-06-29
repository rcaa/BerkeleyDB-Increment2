package com.checksum;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.zip.Checksum;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.EntryHeader;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.log.FileReader;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogManager;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.PrintFileReader;

@Aspect
public class ChecksumFeature {

	private ChecksumValidator validator = null;
	
	@Pointcut("execution(com.sleepycat.je.log.FileReader.new(com.sleepycat.je.dbi.EnvironmentImpl, int, boolean,	long, Long, long,long)) && args(env,int, boolean,	long, Long, long,long) && this(fileReader) && within(com.sleepycat.je.log.FileReader)")
	public void fileReaderConstructor(FileReader fileReader, EnvironmentImpl env) {
	}

	@Around("fileReaderConstructor(fileReader, env)")
	public Object around1(FileReader fileReader, EnvironmentImpl env,
			ProceedingJoinPoint pjp) throws Throwable {
		fileReader.doValidateChecksum = env.getLogManager().getChecksumOnRead();
		Object[] objs = new Object[] { fileReader, env };
		Object r = pjp.proceed(objs);

		if (fileReader.doValidateChecksum) {
			fileReader.cksumValidator = new ChecksumValidator();
		}
		fileReader.anticipateChecksumErrors = false;
		return r;
	}

	@Pointcut("call(void com.sleepycat.je.log.FileReader.hook_checksumValidation(java.nio.ByteBuffer)) && target(fileReader) && args(dataBuffer)")
	public void hook_checksumValidation(FileReader fileReader,
			ByteBuffer dataBuffer) {
	}

	@After("hook_checksumValidation(fileReader, dataBuffer)")
	public void after1(FileReader fileReader, ByteBuffer dataBuffer)
			throws DatabaseException {
		boolean doValidate = fileReader.doValidateChecksum
				&& (fileReader.alwaysValidateChecksum || fileReader
						.isTargetEntry(fileReader.currentEntryTypeNum,
								fileReader.currentEntryTypeVersion));
		/* Initialize the checksum with the header. */
		if (doValidate) {
			fileReader.startChecksum(dataBuffer);
		}
		if (doValidate)
			fileReader.currentEntryCollectData = true;
	}

	@Pointcut("execution(void com.sleepycat.je.log.FileReader.hook_checkType(byte)) && args(currentEntryTypeNum) && this(fr)")
	public void hook_checkType(FileReader fr, byte currentEntryTypeNum) {
	}

	@Before("hook_checkType(fr, currentEntryTypeNum)")
	public void before1(FileReader fr, byte currentEntryTypeNum)
			throws DatabaseException {
		if (!LogEntryType.isValidType(currentEntryTypeNum))
			throw new DbChecksumException((fr.anticipateChecksumErrors ? null
					: fr.env), "FileReader read invalid log entry type: "
					+ currentEntryTypeNum);
	}

	@Pointcut("execution(com.sleepycat.je.log.LogManager.new(com.sleepycat.je.dbi.EnvironmentImpl, boolean)) && this(logManager) && args(env,boolean)")
	public void logManagerConstructor(LogManager logManager, EnvironmentImpl env) {
	}

	@After("logManagerConstructor(logManager, env)")
	public void after2(LogManager logManager, EnvironmentImpl env)
			throws DatabaseException {
		/* See if we're configured to do a checksum when reading in objects. */
		DbConfigManager configManager = env.getConfigManager();
		logManager.doChecksumOnRead = configManager
				.getBoolean(EnvironmentParams.LOG_CHECKSUM_READ);
	}

	@Pointcut("execution(java.nio.ByteBuffer com.sleepycat.je.log.LogManager.addPrevOffset(java.nio.ByteBuffer,long,int)) && args(java.nio.ByteBuffer,long,entrySize) && within(com.sleepycat.je.log.LogManager)")
	public void addPrevOffset(int entrySize) {}
	
	@Around("addPrevOffset(entrySize)")
	public ByteBuffer around2(int entrySize, ProceedingJoinPoint pjp) throws Throwable {
		Object[] objs = new Object[] {entrySize};
		ByteBuffer destBuffer = (ByteBuffer) pjp.proceed(objs);

		Checksum checksum = Adler32.makeChecksum();

		/* Now calculate the checksum and write it into the buffer. */
		checksum.update(destBuffer.array(), LogManager.CHECKSUM_BYTES,
				(entrySize - LogManager.CHECKSUM_BYTES));
		LogUtils.writeUnsignedInt(destBuffer, checksum.getValue());
		destBuffer.position(0);
		return destBuffer;
	}
	
	@Pointcut("call(void com.sleepycat.je.log.EntryHeader.readHeader(java.nio.ByteBuffer, boolean)) && this(lm) && args(entryBuffer,boolean) && target(entryHeader)")
	public void readHeader(LogManager lm, ByteBuffer entryBuffer, EntryHeader entryHeader) {}
	
	@After("readHeader(lm, entryBuffer, entryHeader)")
	public void after3(LogManager lm, ByteBuffer entryBuffer, EntryHeader entryHeader) throws DatabaseException {
		/* Read the checksum to move the buffer forward. */
		if (lm.doChecksumOnRead) {
			validator = new ChecksumValidator();
			int oldpos = entryBuffer.position();
			entryBuffer.position(oldpos - LogManager.HEADER_CONTENT_BYTES);
			validator.update(lm.envImpl, entryBuffer,
					LogManager.HEADER_CONTENT_BYTES, false);
			entryBuffer.position(oldpos);
		}
	}
	
	@Pointcut("call(java.nio.ByteBuffer com.sleepycat.je.log.LogManager.getRemainingBuffer(java.nio.ByteBuffer, com.sleepycat.je.log.LogSource, long, com.sleepycat.je.log.EntryHeader)) && target(lm) && args(entryBuffer, com.sleepycat.je.log.LogSource, long, entryHeader) && cflow(getLogEntryFromLogSource(lsn))")
	public void getRemainingBuffer(LogManager lm, ByteBuffer entryBuffer, EntryHeader entryHeader, long lsn) {}
	
	@After("getRemainingBuffer(lm, entryBuffer, entryHeader, lsn)")
	public void after4(LogManager lm, ByteBuffer entryBuffer, EntryHeader entryHeader, long lsn) throws DatabaseException {
		/*
		 * Do entry validation. Run checksum before checking the entry type, it
		 * will be the more encompassing error.
		 */
		if (lm.doChecksumOnRead) {
			/* Check the checksum first. */
			validator.update(lm.envImpl, entryBuffer,
					entryHeader.getEntrySize(), false);
			validator.validate(lm.envImpl, entryHeader.getChecksum(), lsn);
		}
	}
	
	@Pointcut("execution(com.sleepycat.je.log.entry.LogEntry com.sleepycat.je.log.LogManager.getLogEntryFromLogSource(long, com.sleepycat.je.log.LogSource)) && args(lsn, com.sleepycat.je.log.LogSource)")
	public void getLogEntryFromLogSource(long lsn) {}
	
	@Pointcut("execution(void com.sleepycat.je.log.EntryHeader.readHeader(java.nio.ByteBuffer,boolean)) && target(eh) && args(dataBuffer,boolean)")
	public void readHeader2(EntryHeader eh, ByteBuffer dataBuffer) {}
	
	
	@Before("readHeader2(eh, dataBuffer)")
	public void before2(EntryHeader eh, ByteBuffer dataBuffer) {
		/* Get the checksum for this log entry. */
		eh.checksum = LogUtils.getUnsignedInt(dataBuffer);
	}
	
	@Pointcut("execution(com.sleepycat.je.log.LastFileReader.new(..)) && this(fr)")
	public void lasFileReaderConstructor(FileReader fr) {}
	
	@After("lasFileReaderConstructor(fr)")
	public void after5(FileReader fr) {
		/*
		 * Indicate that a checksum error should not shutdown the whole
		 * environment.
		 */
		fr.anticipateChecksumErrors = true;
	}
	
	@Pointcut("(call(com.sleepycat.je.log.INFileReader.new(..)) && withincode(void com.sleepycat.je.recovery.RecoveryManager.readINsAndTrackIds(long))) ||(call(com.sleepycat.je.log.CleanerFileReader.new(..)) && within(com.sleepycat.je.cleaner.FileProcessor))")
	public void iNFileReaderConstructor() {}
	
	@AfterReturning(value="iNFileReaderConstructor()", returning="fr") 
	public void afterreturning1(FileReader fr) {

		/*
		 * RecoveryManager: Validate all entries in at least one full recovery
		 * pass.
		 */
		/* FileProcessor: Validate all entries before ever deleting a file. */
		fr.setAlwaysValidateChecksum(true);
	}
	
	@Pointcut("call(void com.sleepycat.je.log.FileManager.hook_recomputeChecksum(java.nio.ByteBuffer, int, int)) && args(data,recStartPos,itemSize)")
	public void hook_recomputeChecksum(ByteBuffer data, int recStartPos, int itemSize) {}
	
	@After("hook_recomputeChecksum(data,recStartPos,itemSize)")
	public void after6(ByteBuffer data, int recStartPos, int itemSize) {
		Checksum checksum = Adler32.makeChecksum();
		data.position(recStartPos);
		/* Calculate the checksum and write it into the buffer. */
		int nChecksumBytes = itemSize
				+ (LogManager.HEADER_BYTES - LogManager.CHECKSUM_BYTES);
		byte[] checksumBytes = new byte[nChecksumBytes];
		System.arraycopy(data.array(), recStartPos + LogManager.CHECKSUM_BYTES,
				checksumBytes, 0, nChecksumBytes);
		checksum.update(checksumBytes, 0, nChecksumBytes);
		LogUtils.writeUnsignedInt(data, checksum.getValue());
	}
	
	@Pointcut("execution(boolean com.sleepycat.je.log.LastFileReader.readNextEntry())")
	public void readNextEntry() {}
	
	@Around("readNextEntry()") 
	public boolean around3(ProceedingJoinPoint pjp) throws Throwable {
		boolean r = false;
		try {
			r = (Boolean) pjp.proceed();
		} catch (DbChecksumException e) {
			// refined trace
			/*
			 * Tracer.trace(Level.INFO, env, "Found checksum exception while
			 * searching " + " for end of log. Last valid entry is at " +
			 * DbLsn.toString (DbLsn.makeLsn(readBufferFileNum,
			 * lastValidOffset)) + " Bad entry is at " +
			 * DbLsn.makeLsn(readBufferFileNum, currentEntryOffset));
			 */
		}
		return r;
	}

	@Pointcut("execution(boolean com.sleepycat.je.log.FileManager.readAndValidateFileHeader(java.io.RandomAccessFile, String, long)) && args(newFile,fileName,long) && target(fm) && within(com.sleepycat.je.log.FileManager)")
	public void readAndValidateFileHeader(RandomAccessFile newFile,
			String fileName, FileManager fm) {}
	
	@Around("readAndValidateFileHeader(newFile, fileName, fm)")
	public boolean around4(RandomAccessFile newFile, String fileName, FileManager fm, ProceedingJoinPoint pjp) throws Throwable {
		try {
			Object[] objs = new Object[] {newFile, fileName, fm};
			return (Boolean) pjp.proceed(objs);
		} catch (DbChecksumException e) {

			/*
			 * Let this exception go as a checksum exception, so it sets the run
			 * recovery state correctly.
			 */
			fm.closeFileInErrorCase(newFile);
			throw new DbChecksumException(fm.envImpl, "Couldn't open file "
					+ fileName, e);
		}
	}
	
	@Pointcut("execution(void com.sleepycat.je.log.FileReader.readHeader(java.nio.ByteBuffer)) && args(dataBuffer) && target(fileReader)")
	public void readHeader3(ByteBuffer dataBuffer, FileReader fileReader) {}
	
	@Before("readHeader3(dataBuffer, fileReader)")
	public void before3(ByteBuffer dataBuffer, FileReader fileReader) {
			/* Get the checksum for this log entry. */
			fileReader.currentEntryChecksum = LogUtils.getUnsignedInt(dataBuffer);
	}
	
	@Pointcut("call(java.nio.ByteBuffer java.nio.ByteBuffer.allocate(int)) && withincode(java.nio.ByteBuffer com.sleepycat.je.log.LogManager.marshallIntoBuffer(com.sleepycat.je.log.LoggableObject, int, boolean, int))")
	public void allocate() {}
	
	@AfterReturning(value="allocate()", returning="buffer")
	public void after7(ByteBuffer buffer) {
		/* Reserve 4 bytes at the head for the checksum. */
		buffer.position(LogManager.CHECKSUM_BYTES);
	}
	
	@Pointcut("call(void com.sleepycat.je.log.PrintFileReader.hook_printChecksum(StringBuffer)) && args(sb) && this(pfr)")
	public void hook_printChecksum(StringBuffer sb, PrintFileReader pfr) {}
	
	@After("hook_printChecksum(sb, pfr)")
	public void after8(StringBuffer sb, PrintFileReader pfr) {
		sb.append("\" cksum=\"").append(pfr.currentEntryChecksum);
	}
	
	@After("staticinitialization(LogManager)")
	public void after9() {
		LogManager.HEADER_BYTES += LogManager.CHECKSUM_BYTES; // size of entry
		// header

		LogManager.PREV_BYTES = 4; // size of previous field

		LogManager.HEADER_CONTENT_BYTES = LogManager.HEADER_BYTES
				- LogManager.CHECKSUM_BYTES;

		LogManager.HEADER_ENTRY_TYPE_OFFSET += 4;

		LogManager.HEADER_VERSION_OFFSET += 4;

		LogManager.HEADER_PREV_OFFSET += 4;

		LogManager.HEADER_SIZE_OFFSET += 4;
	}
}
