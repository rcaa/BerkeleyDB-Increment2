/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002-2006
 *      Sleepycat Software.  All rights reserved.
 *
 * $Id: CheckpointConfig.java,v 1.1.6.1.2.1 2006/10/19 19:55:29 ckaestne Exp $
 */

package com.sleepycat.je;

/**
 * Javadoc for this public class is generated via the doc templates in the
 * doc_src directory.
 */
public class CheckpointConfig {
	/*
	 * For internal use, to allow null as a valid value for the config
	 * parameter.
	 */
	public final static CheckpointConfig DEFAULT = new CheckpointConfig();

	private boolean force = false;




	private boolean minimizeRecoveryTime = false;

	/**
	 * Javadoc for this public method is generated via the doc templates in the
	 * doc_src directory.
	 */
	public CheckpointConfig() {
	}





	/**
	 * Javadoc for this public method is generated via the doc templates in the
	 * doc_src directory.
	 */
	public void setForce(boolean force) {
		this.force = force;
	}

	/**
	 * Javadoc for this public method is generated via the doc templates in the
	 * doc_src directory.
	 */
	public boolean getForce() {
		return force;
	}

	/**
	 * Javadoc for this public method is generated via the doc templates in the
	 * doc_src directory.
	 */
	public void setMinimizeRecoveryTime(boolean minimizeRecoveryTime) {
		this.minimizeRecoveryTime = minimizeRecoveryTime;
	}

	/**
	 * Javadoc for this public method is generated via the doc templates in the
	 * doc_src directory.
	 */
	public boolean getMinimizeRecoveryTime() {
		return minimizeRecoveryTime;
	}
}
