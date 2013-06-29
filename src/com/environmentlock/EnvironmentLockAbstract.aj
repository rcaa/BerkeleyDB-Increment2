package com.environmentlock;

import java.io.File;
import java.io.IOException;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.cleaner.Cleaner;
import com.sleepycat.je.log.*;
import com.sleepycat.je.dbi.EnvironmentImpl;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;

public privileged aspect EnvironmentLockAbstract {

	pointcut hook_lockEnvironment(Cleaner c) : execution(boolean Cleaner.hook_lockEnvironment()) && this(c);
	
	pointcut fileManagerConstructor(boolean readOnly, FileManager fm, File dbEnvHome) 
	: execution(FileManager.new(EnvironmentImpl,File,boolean)) && 
		args(EnvironmentImpl, dbEnvHome,readOnly) && this(fm);
	
	pointcut hook_releaseEnvironment(Cleaner c) : execution(void Cleaner.hook_releaseEnvironment()) && this(c);
	
	pointcut close(FileManager fm) : execution(void FileManager.close()) && this(fm);
	
	boolean around(Cleaner c) throws DatabaseException : hook_lockEnvironment(c) {
		if (!proceed(c))
			return false;
		/*
		 * If we can't get an exclusive lock, then there are reader processes
		 * and we can't delete any cleaned files.
		 */
		if (!c.env.getFileManager().lockEnvironment(false, true)) {
			// refined trace Tracer.trace(Level.SEVERE, env, "Cleaner has "
			// + safeFiles.size() +" files not deleted because of read-only
			// processes.");
			return false;
		}
		return true;
	}

	before(boolean readOnly, FileManager fm, File dbEnvHome)
			throws DatabaseException : fileManagerConstructor(readOnly, fm, dbEnvHome) {
		fm.dbEnvHome = dbEnvHome;
		fm.lockEnvironment(readOnly, false);
	}

	before(Cleaner c) throws DatabaseException : hook_releaseEnvironment(c) {
		c.env.getFileManager().releaseExclusiveLock();
	}

	after(FileManager fm) throws IOException, DatabaseException : close(fm) {
		if (fm.exclLock != null) {
			fm.exclLock.release();
		}
		if (fm.lockFile != null) {
			fm.lockFile.close();
		}
		if (fm.channel != null) {
			fm.channel.close();
		}
		if (fm.envLock != null) {
			fm.envLock.release();
		}
	}
	
	/* The channel and lock for the je.lck file. */
	private RandomAccessFile FileManager.lockFile;

	private FileChannel FileManager.channel;

	private FileLock FileManager.envLock;

	private FileLock FileManager.exclLock;
	
	/**
	 * Lock the environment. Return true if the lock was acquired. If exclusive
	 * is false, then this implements a single writer, multiple reader lock. If
	 * exclusive is true, then implement an exclusive lock.
	 * 
	 * There is a lock file and there are two regions of the lock file: byte 0,
	 * and byte 1. Byte 0 is the exclusive writer process area of the lock file.
	 * If an environment is opened for write, then it attempts to take an
	 * exclusive write lock on byte 0. Byte 1 is the shared reader process area
	 * of the lock file. If an environment is opened for read-only, then it
	 * attempts to take a shared lock on byte 1. This is how we implement single
	 * writer, multi reader semantics.
	 * 
	 * The cleaner, each time it is invoked, attempts to take an exclusive lock
	 * on byte 1. The owning process already either has an exclusive lock on
	 * byte 0, or a shared lock on byte 1. This will necessarily conflict with
	 * any shared locks on byte 1, even if it's in the same process and there
	 * are no other holders of that shared lock. So if there is only one
	 * read-only process, it will have byte 1 for shared access, and the cleaner
	 * can not run in it because it will attempt to get an exclusive lock on
	 * byte 1 (which is already locked for shared access by itself). If a write
	 * process comes along and tries to run the cleaner, it will attempt to get
	 * an exclusive lock on byte 1. If there are no other reader processes (with
	 * shared locks on byte 1), and no other writers (which are running cleaners
	 * on with exclusive locks on byte 1), then the cleaner will run.
	 */
	// FEATURE environment locking is a feature
	public boolean FileManager.lockEnvironment(boolean readOnly,
			boolean exclusive) throws DatabaseException {

		try {
			if (checkEnvHomePermissions(readOnly)) {
				return true;
			}
			if (lockFile == null) {
				lockFile = new RandomAccessFile(new File(dbEnvHome, "je"
						+ LOCK_SUFFIX), "rw");
			}
			channel = lockFile.getChannel();

			boolean throwIt = false;
			try {
				if (exclusive) {

					/*
					 * To lock exclusive, must have exclusive on shared reader
					 * area (byte 1).
					 */
					exclLock = channel.tryLock(1, 2, false);
					if (exclLock == null) {
						return false;
					}
					return true;
				} else {
					if (readOnly) {
						envLock = channel.tryLock(1, 2, true);
					} else {
						envLock = channel.tryLock(0, 1, false);
					}
					if (envLock == null) {
						throwIt = true;
					}
				}
			} catch (OverlappingFileLockException e) {
				throwIt = true;
			}
			if (throwIt) {
				throw new LogException("A je" + LOCK_SUFFIX + "file exists in "
						+ dbEnvHome + " The environment can not be locked for "
						+ (readOnly ? "shared" : "single writer") + " access.");
			}
		} catch (IOException IOE) {
			throw new LogException(IOE.toString());
		}
		return true;
	}

	public void FileManager.releaseExclusiveLock() throws DatabaseException {

		try {
			if (exclLock != null) {
				exclLock.release();
			}
		} catch (IOException IOE) {
			throw new DatabaseException(IOE);
		}
	}
}
