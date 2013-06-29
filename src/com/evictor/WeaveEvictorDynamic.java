package com.evictor;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;

import driver.Driver;
import driver.Util;

@Aspect
public class WeaveEvictorDynamic {
	@Around("adviceexecution() && within(com.evictor.WeaveEvictorFeature)")
	public Object adviceexecutionIdiom(JoinPoint thisJoinPoint,
			ProceedingJoinPoint pjp) throws Throwable {
		Object ret;
		if (Driver.isActivated("evictor")) {
			ret = pjp.proceed();
		} else {
			ret = Util.proceedAroundCallAtAspectJ(thisJoinPoint);
		}
		return ret;
	}
}
