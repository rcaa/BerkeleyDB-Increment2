package com.incompressor;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;

import driver.Driver;
import driver.Util;

@Aspect
public class INCompressorDynamic {
	@Around("adviceexecution() && within(com.incompressor.INCompressorFeature)")
	public Object adviceexecutionIdiom(JoinPoint thisJoinPoint,
			ProceedingJoinPoint pjp) throws Throwable {
		Object ret;
		if (Driver.isActivated("incompressor")) {
			ret = pjp.proceed();
		} else {
			ret = Util.proceedAroundCallAtAspectJ(thisJoinPoint);
		}
		return ret;
	}
}
