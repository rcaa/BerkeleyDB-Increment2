/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002-2006
 *      Sleepycat Software.  All rights reserved.
 *
 * $Id: TestHookExecute.java,v 1.1.6.1 2006/07/28 09:02:49 ckaestne Exp $
 */

package com.sleepycat.je.utilint;

/**
 */
public class TestHookExecute {
	public static boolean doHookIfSet(TestHook testHook) {
		if (testHook != null) {
			testHook.doHook();
		}
		return true;
	}
}
