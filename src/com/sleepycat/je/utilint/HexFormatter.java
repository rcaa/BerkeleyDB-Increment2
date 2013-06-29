/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002-2006
 *      Sleepycat Software.  All rights reserved.
 *
 * $Id: HexFormatter.java,v 1.1.6.1 2006/07/28 09:02:51 ckaestne Exp $
 */

package com.sleepycat.je.utilint;

public class HexFormatter {
	static public String formatLong(long l) {
		StringBuffer sb = new StringBuffer();
		sb.append(Long.toHexString(l));
		sb.insert(0, "0000000000000000".substring(0, 16 - sb.length()));
		sb.insert(0, "0x");
		return sb.toString();
	}
}
