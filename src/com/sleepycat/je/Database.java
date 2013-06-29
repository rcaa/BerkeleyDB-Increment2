/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002-2006
 *      Sleepycat Software.  All rights reserved.
 *
 * $Id: Database.java,v 1.4.6.6.2.7 2007/03/01 23:15:35 ckaestne Exp $
 */

package com.sleepycat.je;


import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.GetMode;
import com.sleepycat.je.dbi.PutMode;
import com.sleepycat.je.dbi.TruncateResult;
import com.sleepycat.je.dbi.CursorImpl.SearchMode;
import com.sleepycat.je.txn.Locker;
import com.sleepycat.je.txn.LockerFactory;
import com.sleepycat.je.utilint.TinyHashSet;

public class Database {

	/*
	 * DbState embodies the Database handle state.
	 */
	public static class DbState {
		private String stateName;

		DbState(String stateName) {
			this.stateName = stateName;
		}

		public String toString() {
			return "DbState." + stateName;
		}
	}

	static DbState OPEN = new DbState("OPEN");

	static DbState CLOSED = new DbState("CLOSED");

	static DbState INVALID = new DbState("INVALID");

	/* The current state of the handle. */
	private DbState state;

	/* Handles onto the owning environment and the databaseImpl object. */
	Environment envHandle; // used by subclasses

	private DatabaseImpl databaseImpl;

	DatabaseConfig configuration; // properties used at execution

	/* True if this handle permits write operations; */
	private boolean isWritable;

	/* Transaction that owns the db lock held while the Database is open. */
	Locker handleLocker;

	/* Set of cursors open against this db handle. */
	private TinyHashSet cursors = new TinyHashSet();

	/*
	 * DatabaseTrigger list. The list is null if empty, and is checked for null
	 * to avoiding read locking overhead when no triggers are present. Access to
	 * this list is protected by the shared trigger latch in EnvironmentImpl.
	 */
	private List triggerList;

	private Logger logger;

	/**
	 * Creates a database but does not open or fully initialize it. Is protected
	 * for use in compat package.
	 */
	protected Database(Environment env) {
		this.envHandle = env;
		handleLocker = null;
		// logger = envHandle.getEnvironmentImpl().getLogger();
	}

	/**
	 * Create a database, called by Environment.
	 */
	void initNew(Environment env, Locker locker, String databaseName,
			DatabaseConfig dbConfig) throws DatabaseException {

		if (dbConfig.getReadOnly()) {
			throw new DatabaseException(
					"DatabaseConfig.setReadOnly() must be set to false "
							+ "when creating a Database");
		}

		init(env, dbConfig);

		/* Make the databaseImpl. */
		EnvironmentImpl environmentImpl = DbInternal
				.envGetEnvironmentImpl(envHandle);
		databaseImpl = environmentImpl.createDb(locker, databaseName, dbConfig,
				this);
		databaseImpl.addReferringHandle(this);
	}

	/**
	 * Open a database, called by Environment.
	 */
	void initExisting(Environment env, Locker locker,
			DatabaseImpl databaseImpl, DatabaseConfig dbConfig)
			throws DatabaseException {

		/*
		 * Make sure the configuration used for the open is compatible with the
		 * existing databaseImpl.
		 */
		validateConfigAgainstExistingDb(dbConfig, databaseImpl);

		init(env, dbConfig);
		this.databaseImpl = databaseImpl;
		databaseImpl.addReferringHandle(this);

		/*
		 * Copy the duplicates and transactional properties of the underlying
		 * database, in case the useExistingConfig property is set.
		 */
		configuration.setSortedDuplicates(databaseImpl.getSortedDuplicates());
		// ChK_reintroduced
		// configuration.setTransactional(databaseImpl.isTransactional());
	}

	private void init(Environment env, DatabaseConfig config)
			throws DatabaseException {

		handleLocker = null;

		envHandle = env;
		configuration = config.cloneConfig();
		isWritable = !configuration.getReadOnly();
		state = OPEN;
	}

	/**
	 * See if this new handle's configuration is compatible with the
	 * pre-existing database.
	 */
	private void validateConfigAgainstExistingDb(DatabaseConfig config,
			DatabaseImpl databaseImpl) throws DatabaseException {

		/*
		 * The allowDuplicates property is persistent and immutable. It does not
		 * need to be specified if the useExistingConfig property is set.
		 */
		if (!config.getUseExistingConfig()) {
			if (databaseImpl.getSortedDuplicates() != config
					.getSortedDuplicates()) {
				throw new DatabaseException(
						"You can't open a Database with a duplicatesAllowed "
								+ "configuration of "
								+ config.getSortedDuplicates()
								+ " if the underlying database was created with a "
								+ "duplicatesAllowedSetting of "
								+ databaseImpl.getSortedDuplicates() + ".");
			}
		}

		// ChK_reintroduced removed transaction and reintroduced

		/*
		 * Only re-set the comparators if the override is allowed.
		 */
		if (config.getOverrideBtreeComparator()) {
			databaseImpl.setBtreeComparator(config.getBtreeComparator());
		}

		if (config.getOverrideDuplicateComparator()) {
			databaseImpl
					.setDuplicateComparator(config.getDuplicateComparator());
		}
	}

	public synchronized void close() throws DatabaseException {

		StringBuffer errors = null;

		checkEnv();
		checkProhibitedDbState(CLOSED, "Can't close Database:");

		// refined trace trace(Level.FINEST, "Database.close: ", null, null);

		/* Disassociate triggers before closing. */
		removeAllTriggers();

		envHandle.removeReferringHandle(this);
		if (cursors.size() > 0) {
			errors = new StringBuffer(
					"There are open cursors against the database.\n");
			errors.append("They will be closed.\n");

			/*
			 * Copy the cursors set before iterating since the dbc.close()
			 * mutates the set.
			 */
			Iterator iter = cursors.copy().iterator();
			while (iter.hasNext()) {
				Cursor dbc = (Cursor) iter.next();

				try {
					dbc.close();
				} catch (DatabaseException DBE) {
					errors.append("Exception while closing cursors:\n");
					errors.append(DBE.toString());
				}
			}
		}

		if (databaseImpl != null) {
			databaseImpl.removeReferringHandle(this);
			databaseImpl = null;

			/*
			 * Tell our protecting txn that we're closing. If this type of
			 * transaction doesn't live beyond the life of the handle, it will
			 * release the db handle lock.
			 */
			handleLocker.setHandleLockOwner(true, this, true);
			handleLocker.operationEnd(true);
			state = CLOSED;
		}

		if (errors != null) {
			throw new DatabaseException(errors.toString());
		}
	}

	/**
	 * Javadoc for this public method is generated via the doc templates in the
	 * doc_src directory.
	 */
	public Sequence openSequence(OperationContext cxt, DatabaseEntry key,
			SequenceConfig config) throws DatabaseException {

		checkEnv();
		DatabaseUtil.checkForNullDbt(key, "key", true);
		checkRequiredDbState(OPEN, "Can't call Database.openSequence:");
		checkWritable("openSequence");
		// refined trace trace(Level.FINEST, "Database.openSequence", txn, key,
		// null, null);

		return new Sequence(this, cxt, key, config);
	}

	/**
	 * Javadoc for this public method is generated via the doc templates in the
	 * doc_src directory.
	 */
	public void removeSequence(OperationContext cxt, DatabaseEntry key)
			throws DatabaseException {

		delete(cxt, key);
	}

	public synchronized Cursor openCursor(OperationContext cxt, 
	CursorConfig cursorConfig) throws DatabaseException {

		checkEnv();
		checkRequiredDbState(OPEN, "Can't open a cursor");
		CursorConfig useConfig = (cursorConfig == null) ? CursorConfig.DEFAULT
				: cursorConfig;

		if (useConfig.getReadUncommitted() && useConfig.getReadCommitted()) {
			throw new IllegalArgumentException(
					"Only one may be specified: ReadCommitted or ReadUncommitted");
		}

		// refined trace trace(Level.FINEST, "Database.openCursor", txn,
		// cursorConfig);
		Cursor ret = newDbcInstance(cxt, useConfig);

		return ret;
	}

	/**
	 * Is overridden by SecondaryDatabase.
	 */
	Cursor newDbcInstance(OperationContext cxt, CursorConfig cursorConfig)
			throws DatabaseException {

		return new Cursor(this, cxt, cursorConfig);
	}

	public OperationStatus delete(OperationContext cxt, DatabaseEntry key)
			throws DatabaseException {

		checkEnv();
		DatabaseUtil.checkForNullDbt(key, "key", true);
		checkRequiredDbState(OPEN, "Can't call Database.delete:");
		checkWritable("delete");
		// refined trace trace(Level.FINEST, "Database.delete", txn, key, null,
		// null);

		OperationStatus commitStatus = OperationStatus.NOTFOUND;
		Locker locker = null;
		try {
			locker = LockerFactory.getWritableLocker(envHandle,cxt,false);
			commitStatus = deleteInternal(locker, key);
			return commitStatus;
		} finally {
			if (locker != null) {
				locker.operationEnd(commitStatus);
			}
		}
	}

	/**
	 * Internal version of delete() that does no parameter checking. Notify
	 * triggers. Deletes all duplicates.
	 */
	OperationStatus deleteInternal(Locker locker, DatabaseEntry key)
			throws DatabaseException {

		Cursor cursor = null;
		try {
			cursor = new Cursor(this, locker, null);
			cursor.setNonCloning(true);
			OperationStatus commitStatus = OperationStatus.NOTFOUND;

			/* Position a cursor at the specified data record. */
			DatabaseEntry oldData = new DatabaseEntry();
			OperationStatus searchStatus = cursor.search(key, oldData,
					LockMode.RMW, SearchMode.SET);

			/* Delete all records with that key. */
			if (searchStatus == OperationStatus.SUCCESS) {
				do {

					/*
					 * Notify triggers before the actual deletion so that a
					 * primary record never exists while secondary keys refer to
					 * it. This is relied on by secondary read-uncommitted.
					 */
					if (hasTriggers()) {
						notifyTriggers(locker, key, oldData, null);
					}
					/* The actual deletion. */
					commitStatus = cursor.deleteNoNotify();
					if (commitStatus != OperationStatus.SUCCESS) {
						return commitStatus;
					}
					/* Get another duplicate. */
					if (databaseImpl.getSortedDuplicates()) {
						searchStatus = cursor.retrieveNext(key, oldData,
								LockMode.RMW, GetMode.NEXT_DUP);
					} else {
						searchStatus = OperationStatus.NOTFOUND;
					}
				} while (searchStatus == OperationStatus.SUCCESS);
				commitStatus = OperationStatus.SUCCESS;
			}
			return commitStatus;
		} finally {
			if (cursor != null) {
				cursor.close();
			}
		}
	}

	public OperationStatus get(OperationContext cxt, DatabaseEntry key,
			DatabaseEntry data, LockMode lockMode) throws DatabaseException {

		checkEnv();
		DatabaseUtil.checkForNullDbt(key, "key", true);
		DatabaseUtil.checkForNullDbt(data, "data", false);
		checkRequiredDbState(OPEN, "Can't call Database.get:");
		// refined trace trace(Level.FINEST, "Database.get", txn, key, null,
		// lockMode);

		CursorConfig cursorConfig = CursorConfig.DEFAULT;
		if (lockMode == LockMode.READ_COMMITTED) {
			cursorConfig = CursorConfig.READ_COMMITTED;
			lockMode = null;
		}

		Cursor cursor = null;
		try {
			cursor = new Cursor(this, cxt, cursorConfig);
			cursor.setNonCloning(true);
			return cursor.search(key, data, lockMode, SearchMode.SET);
		} finally {
			if (cursor != null) {
				cursor.close();
			}
		}
	}

	public OperationStatus getSearchBoth(
	OperationContext cxt, DatabaseEntry key, DatabaseEntry data,
			LockMode lockMode) throws DatabaseException {

		checkEnv();
		DatabaseUtil.checkForNullDbt(key, "key", true);
		DatabaseUtil.checkForNullDbt(data, "data", true);
		checkRequiredDbState(OPEN, "Can't call Database.getSearchBoth:");
		// refined trace trace(Level.FINEST, "Database.getSearchBoth", txn, key,
		// data, lockMode);

		CursorConfig cursorConfig = CursorConfig.DEFAULT;
		if (lockMode == LockMode.READ_COMMITTED) {
			cursorConfig = CursorConfig.READ_COMMITTED;
			lockMode = null;
		}

		Cursor cursor = null;
		try {
			cursor = new Cursor(this, cxt, cursorConfig);
			cursor.setNonCloning(true);
			return cursor.search(key, data, lockMode, SearchMode.BOTH);
		} finally {
			if (cursor != null) {
				cursor.close();
			}
		}
	}

	public OperationStatus put(OperationContext cxt, DatabaseEntry key,
			DatabaseEntry data) throws DatabaseException {

		checkEnv();
		DatabaseUtil.checkForNullDbt(key, "key", true);
		DatabaseUtil.checkForNullDbt(data, "data", true);
		DatabaseUtil.checkForPartialKey(key);
		checkRequiredDbState(OPEN, "Can't call Database.put");
		checkWritable("put");
		// refined trace trace(Level.FINEST, "Database.put", txn, key, data,
		// null);

		return putInternal(cxt, key, data, PutMode.OVERWRITE);
	}

	public OperationStatus putNoOverwrite(
	OperationContext cxt, DatabaseEntry key, DatabaseEntry data)
			throws DatabaseException {

		checkEnv();
		DatabaseUtil.checkForNullDbt(key, "key", true);
		DatabaseUtil.checkForNullDbt(data, "data", true);
		DatabaseUtil.checkForPartialKey(key);
		checkRequiredDbState(OPEN, "Can't call Database.putNoOverWrite");
		checkWritable("putNoOverwrite");
		// refined trace trace(Level.FINEST, "Database.putNoOverwrite", txn,
		// key, data, null);

		return putInternal(cxt, key, data, PutMode.NOOVERWRITE);
	}

	public OperationStatus putNoDupData(
	OperationContext cxt, DatabaseEntry key, DatabaseEntry data)
			throws DatabaseException {

		checkEnv();
		DatabaseUtil.checkForNullDbt(key, "key", true);
		DatabaseUtil.checkForNullDbt(data, "data", true);
		DatabaseUtil.checkForPartialKey(key);
		checkRequiredDbState(OPEN, "Can't call Database.putNoDupData");
		checkWritable("putNoDupData");
		// refined trace trace(Level.FINEST, "Database.putNoDupData", txn, key,
		// data, null);

		return putInternal(cxt, key, data, PutMode.NODUP);
	}

	/**
	 * Internal version of put() that does no parameter checking.
	 */
	OperationStatus putInternal(OperationContext cxt, DatabaseEntry key,
			DatabaseEntry data, PutMode putMode) throws DatabaseException {

		Locker locker = null;
		Cursor cursor = null;
		OperationStatus commitStatus = OperationStatus.KEYEXIST;
		try {
			locker = LockerFactory.getWritableLocker(envHandle,cxt,false);

			cursor = new Cursor(this, locker, null);
			cursor.setNonCloning(true);
			commitStatus = cursor.putInternal(key, data, putMode);
			return commitStatus;
		} finally {
			if (cursor != null) {
				cursor.close();
			}
			if (locker != null) {
				locker.operationEnd(commitStatus);
			}
		}
	}

	/**
	 */
	public JoinCursor join(Cursor[] cursors, JoinConfig config)
			throws DatabaseException {

		checkEnv();
		checkRequiredDbState(OPEN, "Can't call Database.join");
		DatabaseUtil.checkForNullParam(cursors, "cursors");
		if (cursors.length == 0) {
			throw new IllegalArgumentException(
					"At least one cursor is required.");
		}

		/*
		 * Check that all cursors use the same locker, if any cursor is
		 * transactional. And if non-transactional, that all databases are in
		 * the same environment.
		 */
		EnvironmentImpl env = envHandle.getEnvironmentImpl();
		for (int i = 1; i < cursors.length; i += 1) {
			EnvironmentImpl env2 = cursors[i].getDatabaseImpl()
					.getDbEnvironment();
			if (env != env2) {
				throw new IllegalArgumentException(
						"All cursors must use the same environment.");
			}
		}

		/* Create the join cursor. */
		return new JoinCursor(null, this, cursors, config);
	}

	// TODO removed deprecated methods

	/*
	 * @deprecated As of JE 2.0.55, replaced by
	 *             {@link Database#preload(PreloadConfig)}.
	 */
	public void preload(long maxBytes) throws DatabaseException {

		checkEnv();
		checkRequiredDbState(OPEN, "Can't call Database.preload");

		PreloadConfig config = new PreloadConfig();
		hook_setConfig(config,maxBytes);
		databaseImpl.preload(config);
	}

	private void hook_setConfig(PreloadConfig config, long maxBytes) {
		// TODO Auto-generated method stub
		
	}

	/*
	 * @deprecated As of JE 2.1.1, replaced by
	 *             {@link Database#preload(PreloadConfig)}.
	 */
	public void preload(long maxBytes, long maxMillisecs)
			throws DatabaseException {

		checkEnv();
		checkRequiredDbState(OPEN, "Can't call Database.preload");
		

		PreloadConfig config = new PreloadConfig();
		hook_setConfig(config,maxBytes);
		config.setMaxMillisecs(maxMillisecs);
		databaseImpl.preload(config);
	}

	public PreloadResult preload(PreloadConfig config) throws DatabaseException {

		checkEnv();
		checkRequiredDbState(OPEN, "Can't call Database.preload");

		return databaseImpl.preload(config);
	}




	public String getDatabaseName() throws DatabaseException {

		checkEnv();
		if (databaseImpl != null) {
			return databaseImpl.getName();
		} else {
			return null;
		}
	}

	/*
	 * Non-transactional database name, safe to access when creating error
	 * messages.
	 */
	String getDebugName() {
		if (databaseImpl != null) {
			return databaseImpl.getDebugName();
		} else {
			return null;
		}
	}

	public DatabaseConfig getConfig() throws DatabaseException {

		DatabaseConfig showConfig = configuration.cloneConfig();

		/*
		 * Set the comparators from the database impl, they might have changed
		 * from another handle.
		 */
		Comparator btComp = (databaseImpl == null ? null : databaseImpl
				.getBtreeComparator());
		Comparator dupComp = (databaseImpl == null ? null : databaseImpl
				.getDuplicateComparator());
		showConfig
				.setBtreeComparator(btComp == null ? null : btComp.getClass());
		showConfig.setDuplicateComparator(dupComp == null ? null : dupComp
				.getClass());
		return showConfig;
	}

	public Environment getEnvironment() throws DatabaseException {

		return envHandle;
	}

	public List getSecondaryDatabases() throws DatabaseException {

		List list = new ArrayList();
		if (hasTriggers()) {
			for (int i = 0; i < triggerList.size(); i += 1) {
				Object obj = triggerList.get(i);
				if (obj instanceof SecondaryTrigger) {
					list.add(((SecondaryTrigger) obj).getDb());
				}
			}
		} else {
		}
		return list;
	}

	/*
	 * Helpers, not part of the public API
	 */

	/**
	 * @return true if the Database was opened read/write.
	 */
	boolean isWritable() {
		return isWritable;
	}

	/**
	 * Return the databaseImpl object instance.
	 */
	DatabaseImpl getDatabaseImpl() {
		return databaseImpl;
	}

	/**
	 * The handleLocker is the one that holds the db handle lock.
	 */
	void setHandleLocker(Locker locker) {
		handleLocker = locker;
	}

	synchronized void removeCursor(Cursor dbc) {
		cursors.remove(dbc);
	}

	synchronized void addCursor(Cursor dbc) {
		cursors.add(dbc);
	}

	/**
	 * @throws DatabaseException
	 *             if the Database state is not this value.
	 */
	void checkRequiredDbState(DbState required, String msg)
			throws DatabaseException {

		if (state != required) {
			throw new DatabaseException(msg + " Database state can't be "
					+ state + " must be " + required);
		}
	}

	/**
	 * @throws DatabaseException
	 *             if the Database state is this value.
	 */
	void checkProhibitedDbState(DbState prohibited, String msg)
			throws DatabaseException {

		if (state == prohibited) {
			throw new DatabaseException(msg + " Database state must not be "
					+ prohibited);
		}
	}

	/**
	 * @throws RunRecoveryException
	 *             if the underlying environment is invalid
	 */
	void checkEnv() throws RunRecoveryException {

		EnvironmentImpl env = envHandle.getEnvironmentImpl();
		if (env != null) {
			env.checkIfInvalid();
		}
	}

	/**
	 * Invalidate the handle, called by txn.abort by way of DbInternal.
	 */
	synchronized void invalidate() {
		state = INVALID;
		envHandle.removeReferringHandle(this);
		if (databaseImpl != null) {
			databaseImpl.removeReferringHandle(this);
		}
	}

	/**
	 * Check that write operations aren't used on a readonly Database.
	 */
	private void checkWritable(String operation) throws DatabaseException {

		if (!isWritable) {
			throw new DatabaseException("Database is Read Only: " + operation);
		}
	}

	// TODO trace
	// /**
	// * Send trace messages to the java.util.logger. Don't rely on the logger
	// * alone to conditionalize whether we send this message, we don't even
	// want
	// * to construct the message if the level is not enabled.
	// */
	// void trace(Level level, String methodName, /*ChK Transaction txn,
	// DatabaseEntry key, DatabaseEntry data, LockMode lockMode)
	// throws DatabaseException {
	//
	// if (logger.isLoggable(level)) {
	// StringBuffer sb = new StringBuffer();
	// sb.append(methodName);
	// if (txn != null) {
	// sb.append(" txnId=").append(txn.getId());
	// }
	// sb.append(" key=").append(key.dumpData());
	// if (data != null) {
	// sb.append(" data=").append(data.dumpData());
	// }
	// if (lockMode != null) {
	// sb.append(" lockMode=").append(lockMode);
	// }
	// logger.log(level, sb.toString());
	// }
	// }
	//
	// /**
	// * Send trace messages to the java.util.logger. Don't rely on the logger
	// * alone to conditionalize whether we send this message, we don't even
	// want
	// * to construct the message if the level is not enabled.
	// */
	// void trace(Level level, String methodName, Transaction txn,
	// CursorConfig config) throws DatabaseException {
	//
	// if (logger.isLoggable(level)) {
	// StringBuffer sb = new StringBuffer();
	// sb.append(methodName);
	// sb.append(" name=" + getDebugName());
	// if (txn != null) {
	// sb.append(" txnId=").append(txn.getId());
	// }
	// if (config != null) {
	// sb.append(" config=").append(config);
	// }
	// logger.log(level, sb.toString());
	// }
	// }

	/*
	 * Manage triggers.
	 */

	/**
	 * Returns whether any triggers are currently associated with this primary.
	 * Note that an update of the trigger list may be in progress and this
	 * method does not wait for that update to be completed.
	 */
	boolean hasTriggers() {

		return triggerList != null;
	}

	/**
	 * Adds a given trigger to the list of triggers. Called while opening a
	 * SecondaryDatabase.
	 * 
	 * @param insertAtFront
	 *            true to insert at the front, or false to append.
	 */
	void addTrigger(DatabaseTrigger trigger, boolean insertAtFront)
			throws DatabaseException {

		if (triggerList == null) {
			triggerList = new ArrayList();
		}
		if (insertAtFront) {
			triggerList.add(0, trigger);
		} else {
			triggerList.add(trigger);
		}
		trigger.triggerAdded(this);
	}

	/**
	 * Removes a given trigger from the list of triggers. Called by
	 * SecondaryDatabase.close().
	 */
	void removeTrigger(DatabaseTrigger trigger) throws DatabaseException {
		if (triggerList == null) {
			triggerList = new ArrayList();
		}

		triggerList.remove(trigger);
		trigger.triggerRemoved(this);
	}

	/**
	 * Clears the list of triggers. Called by close(), this allows closing the
	 * primary before its secondaries, although we document that secondaries
	 * should be closed first.
	 */
	private void removeAllTriggers() throws DatabaseException {
		if (triggerList == null) {
			triggerList = new ArrayList();
		}

		for (int i = 0; i < triggerList.size(); i += 1) {
			DatabaseTrigger trigger = (DatabaseTrigger) triggerList.get(i);
			trigger.triggerRemoved(this);
		}
		triggerList.clear();
	}

	/**
	 * Notifies associated triggers when a put() or delete() is performed on the
	 * primary. This method is normally called only if hasTriggers() has
	 * returned true earlier. This avoids acquiring a shared latch for primaries
	 * with no triggers. If a trigger is added during the update process, there
	 * is no requirement to immediately start updating it.
	 * 
	 * @param locker
	 *            the internal locker.
	 * 
	 * @param priKey
	 *            the primary key.
	 * 
	 * @param oldData
	 *            the primary data before the change, or null if the record did
	 *            not previously exist.
	 * 
	 * @param newData
	 *            the primary data after the change, or null if the record has
	 *            been deleted.
	 */
	void notifyTriggers(Locker locker, DatabaseEntry priKey,
			DatabaseEntry oldData, DatabaseEntry newData)
			throws DatabaseException {

		for (int i = 0; i < triggerList.size(); i += 1) {
			DatabaseTrigger trigger = (DatabaseTrigger) triggerList.get(i);

			/* Notify trigger. */
			trigger.databaseUpdated(this, locker, priKey, oldData, newData);
		}
	}
}
