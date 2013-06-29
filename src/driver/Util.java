package driver;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;

public class Util {

	public static Object proceedAroundCallAtAspectJ(JoinPoint thisJoinPoint)
			throws Throwable {
		Object ret = null;
		Object args[] = thisJoinPoint.getArgs();
		if (args.length > 0) {
			int proceedIndex = (args.length - 1);
			if (args[proceedIndex] instanceof ProceedingJoinPoint) {
				ret = ((ProceedingJoinPoint) args[proceedIndex]).proceed();
			}
		}
		return ret;
	}
}
