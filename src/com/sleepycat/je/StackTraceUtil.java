package com.sleepycat.je;

import java.io.PrintWriter;
import java.io.StringWriter;

public class StackTraceUtil {
	/**
	 * @return the stacktrace for an exception
	 */
	public static String getStackTrace(Throwable t) {
		StringWriter s = new StringWriter();
		t.printStackTrace(new PrintWriter(s));
		String stackTrace = s.toString();
		stackTrace = stackTrace.replaceAll("<", "&lt;");
		stackTrace = stackTrace.replaceAll(">", "&gt;");
		return stackTrace;
	}
}
