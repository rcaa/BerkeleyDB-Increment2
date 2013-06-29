package com.sleepycat.je.log;

import com.sleepycat.je.RunRecoveryException;
import com.sleepycat.je.dbi.EnvironmentImpl;

public class InvalidLogTypeException extends RunRecoveryException {
	public InvalidLogTypeException(EnvironmentImpl env, String message) {
		super(env, message);
	}

	public InvalidLogTypeException(EnvironmentImpl env, String message, Throwable t) {
		super(env, message, t);
	}
}
